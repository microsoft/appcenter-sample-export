namespace AppCenter.Samples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reactive.Concurrency;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using AppCenter.ExportParser;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Newtonsoft.Json;
    using Serilog;

    public static class ExportObservable
    {
        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start)
            => container.CreateExport(start, DateTimeOffset.MaxValue, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish)
            => container.CreateExport(start, finish, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, IScheduler scheduler)
            => container.CreateExport(start, DateTimeOffset.MaxValue, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, IScheduler scheduler)
            => container.CreateExport(start, finish, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, ExportOptions options, IScheduler scheduler)
            => container.CreateExport(start, DateTimeOffset.MaxValue, options, scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, ExportOptions options)
            => container.CreateExport(start, finish, options, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, ExportOptions options)
            => container.CreateExport(start, DateTimeOffset.MaxValue, options, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IEnumerable<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, ExportOptions options, IScheduler scheduler)
        {
            if (start < finish)
                throw new ArgumentException("start must be less than finish", nameof(finish));

            return container.CreateJsonExportObservables(start, finish, options, scheduler)
                .GroupBy(_ => _.Timestamp)
                .SelectMany(_ =>
                    _.SelectMany(__ => __.Observable)
                        .Select(json =>
                        {
                            if (string.IsNullOrEmpty(json))
                                return Array.Empty<DeviceLog>();
                            try
                            {
                                return JsonConvert.DeserializeObject<DeviceLog[]>(json);
                            }
                            catch (Exception exception)
                            {
                                Log.Error(exception, "Error deserializing {JSON} as a device log", json);
                                return Array.Empty<DeviceLog>();
                            }
                        })

                        // TODO: The next two operators (Aggregate/Select) can be omitted for performance if you do not require logs
                        //       grouped by minute, or ordered by ingress time
                        .Aggregate(
                            new List<DeviceLog>() as IEnumerable<DeviceLog>,
                            (accumulator, newLogs) => accumulator.Concat(newLogs),
                            accumulator => accumulator.OrderBy(log => log.IngressTimestamp))
                        .Select(logs => (Timestamp: _.Key, Logs: logs as IEnumerable<DeviceLog>)));
        }

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish)
            => container.CreateJsonExport(start, finish, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start)
            => container.CreateJsonExport(start, DateTimeOffset.MaxValue, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, finish, new ExportOptions(), scheduler)
                .SelectMany(_ => _.Observable.Select(json => (Timestamp: _.Timestamp, Json: json)));

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, DateTimeOffset.MaxValue, new ExportOptions(), scheduler)
                .SelectMany(_ => _.Observable.Select(json => (Timestamp: _.Timestamp, Json: json)));

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish)
            => container.CreateJsonExportObservables(start, finish, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, finish, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, DateTimeOffset.MaxValue, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, ExportOptions options, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, DateTimeOffset.MaxValue, options, scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, ExportOptions options, IScheduler scheduler)
        {
            if (start < finish)
                throw new ArgumentException("start must be less than finish", nameof(finish));

            const int maxResults = 100;
            var step = TimeSpan.FromMinutes(1);
            return Observable
                .Generate(start, timestamp => timestamp < finish, timestamp => timestamp + step, timestamp => timestamp)
                .Delay(dt =>
                {
                    // Ensure we're not too close to the current time to avoid missing exported data
                    var delay = scheduler.Now - dt;
                    if (delay < options.MinLatency)
                        delay = options.MinLatency;
                    return Observable.Timer(delay);
                })
                .Select(timestamp =>
                {
                    var observable = container
                        .CreateMinuteJsonExport(timestamp, maxResults, options, scheduler)
                        .SelectMany(blob =>
                        {
                            try
                            {
                                return blob.DownloadTextAsync();
                            }
                            catch (Exception exception)
                            {
                                Log.Error(exception, "Error downloading from {SourceBLOB}", blob.Uri);
                                return Task.FromResult(string.Empty);
                            }
                        });
                    return (Timestamp: timestamp, Observable: observable);
                });
        }

        public static IObservable<CloudBlockBlob> CreateMinuteJsonExport(this CloudBlobContainer container, DateTimeOffset timestamp, int maxResults, ExportOptions options, IScheduler scheduler)
        {
            const bool useFlatBlobListing = true;
            const BlobListingDetails blobListingDetails = BlobListingDetails.None;
            var prefix = $"{timestamp:yyyy/MM/dd/HH/mm}/logs";
            return Observable.Create<CloudBlockBlob>(observer =>
            {
                return scheduler.ScheduleAsync(async (schdlr, cancellationToken) =>
                {
                    try
                    {
                        BlobContinuationToken continuationToken = null;
                        do
                        {
                            var segment = await GetNextBlobSegmentAsync(continuationToken, cancellationToken).ConfigureAwait(false);
                            PublishBlobs(observer, segment.Results);
                            continuationToken = segment.ContinuationToken;
                        } while (!(cancellationToken.IsCancellationRequested || continuationToken is null));

                        observer.OnCompleted();
                    }
                    catch (OperationCanceledException exception) when (exception.CancellationToken == cancellationToken)
                    {
                        observer.OnCompleted();
                    }
                    catch (Exception exception)
                    {
                        observer.OnError(exception);
                    }
                });
            });

            Task<BlobResultSegment> GetNextBlobSegmentAsync(BlobContinuationToken continuationToken, CancellationToken cancellationToken)
                => container.ListBlobsSegmentedAsync(prefix, useFlatBlobListing, blobListingDetails, maxResults, continuationToken, options.BlobRequestOptions, options.OperationContext, cancellationToken);

            void PublishBlobs(IObserver<CloudBlockBlob> observer, IEnumerable<IListBlobItem> blobs)
            {
                foreach (var blob in blobs.Where(b => b.Uri.AbsolutePath.EndsWith("v1.data")).OfType<CloudBlockBlob>())
                    observer.OnNext(blob);
            }
        }
    }
}
