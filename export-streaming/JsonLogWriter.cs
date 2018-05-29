namespace AppCenter.Samples
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Newtonsoft.Json;
    using Serilog;

    public interface IWriter
    {
        Task WriteAsync<T>(DateTimeOffset timestamp, IEnumerable<T> items, CancellationToken cancellationToken);
    }

    public class JsonBlobWriter : IWriter
    {
        public JsonBlobWriter(CloudBlobContainer container, JsonSerializer serializer, ExportOptions options, Encoding encoding, int bufferSize = 1024)
        {
            Container = container ?? throw new ArgumentNullException(nameof(container));
            Encoding = encoding ?? Encoding.UTF8;
            Options = options ?? throw new ArgumentNullException(nameof(options));
            Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            BufferSize = bufferSize;
        }

        private AccessCondition AccessCondition => Options.AccessCondition;

        private BlobRequestOptions BlobRequestOptions => Options.BlobRequestOptions;

        private OperationContext OperationContext => Options.OperationContext;

        private int BufferSize { get; }

        private CloudBlobContainer Container { get; }

        private Encoding Encoding { get; }

        private ExportOptions Options { get; }

        private JsonSerializer Serializer { get; }

        public async Task WriteAsync<T>(DateTimeOffset timestamp, IEnumerable<T> items, CancellationToken cancellationToken)
        {
            var blobName = $"{timestamp:yyyy/MM/dd/HH/mm}/logs";
            try
            {
                var blob = Container.GetBlockBlobReference(blobName);
                using (var stream = await blob.OpenWriteAsync(AccessCondition, BlobRequestOptions, OperationContext, cancellationToken).ConfigureAwait(false))
                using (var textWriter = new StreamWriter(stream, Encoding, BufferSize, true))
                using (var jsonWriter = new JsonTextWriter(textWriter))
                {
                    await jsonWriter.WriteStartArrayAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        foreach (var item in items.TakeWhile(log => !cancellationToken.IsCancellationRequested))
                            Serializer.Serialize(jsonWriter, item);
                    }
                    finally
                    {
                        await jsonWriter.WriteEndArrayAsync(default).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception exception)
            {
                Log.Error(exception, "Error writing logs to Azure storage BLOB {BLOB}", blobName);
            }
        }
    }
}
