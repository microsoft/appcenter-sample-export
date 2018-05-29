using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AppCenter.ExportParser;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace AppCenter.Samples
{
    public static class ForwardLogs
    {
        [FunctionName("ForwardLogs")]
        public static void Run([BlobTrigger("archive/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, TraceWriter log, ExecutionContext context)
        {
            // Only export logs
            if (!name.EndsWith("v1.data"))
            {
                return;
            }

            // Deserialize blob
            log.Info($"Processing blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            var serializer = new JsonSerializer();
            using (var sr = new StreamReader(myBlob))
            using (var jsonTextReader = new JsonTextReader(sr))
            {
                var logs = serializer.Deserialize<DeviceLog[]>(jsonTextReader);
                // Filter logs
                // TODO: Replace with real filter
                var filteredLogs = logs.Where(l => l.InstallId == "590b1bd0-f75c-4bfe-a006-75d552c9dd37");
                // If any logs to export, process logs
                if (filteredLogs.Any())
                {
                    var inputConnectionString = GetConnectionString(context, "InputConnectionString");
                    var outputConnectionString = GetConnectionString(context, "OutputConnectionString");
                    var forwarder = new ExportForwarder(inputConnectionString, outputConnectionString, "sample2");
                    forwarder.ForwardLogs(name, filteredLogs);
                    log.Info($"Exported {filteredLogs.Count()} logs to {name}");
                }
            }
        }

        private static string GetConnectionString(ExecutionContext context, string connectionName)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            return config.GetConnectionString(connectionName);
        }
    }
}
