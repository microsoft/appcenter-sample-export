using System;
using System.Threading.Tasks;
using AppCenter.ExportParser;

namespace AppCenter.Samples
{
    public class Program
    {
        public static void Main(params string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: sample-console-app.exe <installId> <blob storage connection string where export is pointing to> " +
                    "<blob storage connection string where filtered data will go> <output container name>");
            }
            Console.WriteLine("Forwarding logs:");
            var installId = args[0];
            var inputConnectionString = args[1];
            var outputConnectionString = args[2];
            var outputContainerName = args[3];

            // Creating forwarder
            var forwarder = new ExportForwarder(
                inputConnectionString, 
                outputConnectionString, outputContainerName);
            forwarder.ParallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = 16
            };

            // Forward all logs from 2018-05-01T22:25:00Z through 2018-05-02T22:26:00Z
            // Filtered by InstallId "590b1bd0-f75c-4bfe-a006-75d552c9dd37"
            forwarder.RetrieveAndForwardLogs(
                DateTimeOffset.UtcNow.AddDays(-30), 
                DateTimeOffset.UtcNow,
                l => l.InstallId == installId);

            // Sample: filter by date and event property value
            // forwarder.RetrieveAndForwardLogs(
            //    new DateTimeOffset(2018, 2, 1, 1, 0, 0, 0, TimeSpan.Zero),
            //    new DateTimeOffset(2018, 2, 2, 1, 0, 0, 0, TimeSpan.Zero),
            //    l => l.MessageType == MessageTypes.EventLog && l.Properties.Contains("test@email.com"));

            Console.WriteLine($"Done");
        }
    }
}
