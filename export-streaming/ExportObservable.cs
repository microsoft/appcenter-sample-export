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

namespace AppCenter.Samples
{
    public static class ExportObservable
    {
        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start)
            => container.CreateExport(start, DateTimeOffset.MaxValue, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish)
            => container.CreateExport(start, finish, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, IScheduler scheduler)
            => container.CreateExport(start, DateTimeOffset.MaxValue, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, IScheduler scheduler)
            => container.CreateExport(start, finish, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, ExportOptions options, IScheduler scheduler)
            => container.CreateExport(start, DateTimeOffset.MaxValue, options, scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, ExportOptions options)
            => container.CreateExport(start, finish, options, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, ExportOptions options)
            => container.CreateExport(start, DateTimeOffset.MaxValue, options, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IList<DeviceLog> Logs)> CreateExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, ExportOptions options, IScheduler scheduler)
        {
            if (start > finish)
                throw new ArgumentException("start must be less than finish", nameof(finish));

            return container.CreateJsonExport(start, finish, options, scheduler)
                .Select(_ =>
                {
                    var (timestamp, json) = _;
                    if (string.IsNullOrEmpty(json))
                        return (Timestamp: timestamp, Logs: Array.Empty<DeviceLog>() as IList<DeviceLog>);
                    try
                    {
                        return (Timestamp: timestamp, Logs: JsonConvert.DeserializeObject<DeviceLog[]>(json) as IList<DeviceLog>);
                    }
                    catch (Exception exception)
                    {
                        Log.Error(exception, "Error deserializing {JSON} as a device log", json);
                        return (Timestamp: timestamp, Logs: Array.Empty<DeviceLog>() as IList<DeviceLog>);
                    }
                });
        }

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish)
            => container.CreateJsonExport(start, finish, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start)
            => container.CreateJsonExport(start, DateTimeOffset.MaxValue, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, IScheduler scheduler)
            => container.CreateJsonExport(start, finish, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, ExportOptions options, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, finish, options, scheduler)
                .Select(_ => _.Observable.Select(json => (Timestamp: _.Timestamp, Json: json)))
                .Concat();

        public static IObservable<(DateTimeOffset Timestamp, string Json)> CreateJsonExport(this CloudBlobContainer container, DateTimeOffset start, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, DateTimeOffset.MaxValue, new ExportOptions(), scheduler)
                .Select(_ => _.Observable.Select(json => (Timestamp: _.Timestamp, Json: json)))
                .Concat();

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish)
            => container.CreateJsonExportObservables(start, finish, Scheduler.Default);

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, finish, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, DateTimeOffset.MaxValue, new ExportOptions(), scheduler);

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, ExportOptions options, IScheduler scheduler)
            => container.CreateJsonExportObservables(start, DateTimeOffset.MaxValue, options, scheduler);

        public static IObservable<DateTimeOffset> GetExportTicks(DateTimeOffset start, DateTimeOffset finish, ExportOptions options, IScheduler scheduler)
        {
            return Observable.Create<DateTimeOffset>(observer =>
            {
                return scheduler.ScheduleAsync(
                    start,
                    async (schdlr, timestamp, cancellationToken) =>
                    {
                        try
                        {
                            var step = TimeSpan.FromMinutes(1);
                            while (timestamp < finish)
                            {
                                var now = schdlr.Now;
                                if (timestamp < now - options.MinLatency)
                                    observer.OnNext(timestamp);
                                else
                                    await schdlr.Sleep(now - options.MinLatency, cancellationToken);
                                timestamp += step;
                            }

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
        }

        public static IObservable<(DateTimeOffset Timestamp, IObservable<string> Observable)> CreateJsonExportObservables(this CloudBlobContainer container, DateTimeOffset start, DateTimeOffset finish, ExportOptions options, IScheduler scheduler)
        {
            const int maxResults = 100;
            return GetExportTicks(start, finish, options, scheduler)
                .Select(timestamp =>
                {
                    var observable = container
                        .CreateMinuteJsonExport(timestamp, maxResults, options, scheduler)
                        .Select(blob =>
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
                        })
                        .Concat();
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
                foreach (var blob in blobs.Where(b => b.Uri.AbsolutePath.EndsWith("logs.v1.data")).OfType<CloudBlockBlob>())
                    observer.OnNext(blob);
            }
        }
    }
}
