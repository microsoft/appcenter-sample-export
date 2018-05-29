# App Center Export Parser
This repo contains sample .NET Core library for parsing Export blobs created via App Center Continuous Export. 

## Sample 1 - Console App to forward logs for specific users to their account
This sample is located under `sample-console-app`. The console app can be used to selectively forward logs from one storage account to another. This can be useful for satisfying one of GDPR export requests.

To build the sample:
```
dotnet build export-parser.sln
```

To run the sample:
```
sample-console-app.exe <installId> <blob storage connection string where export is pointing to> <blob storage connection string where filtered data will go> <output container name>
```

Note: since the sample downloads each export blob to a local machine, it will be singnificantly faster if the code was executed on Azure VM in the same data center as the storage account.

## Sample 2 - Azure Function to forward logs for specific users to their account
This sample is located under `sample-azure-function`. This Azure Function is similar to the console app sample, however is can be used to selectively forward logs on an ongoing basis.

To build the sample:
```
dotnet build export-parser.sln
```

To deploy the function refer to [this article](https://docs.microsoft.com/en-us/azure/azure-functions/functions-develop-vs).

## Sample 3 - Continuously stream export data

This sample shows how to continuously stream data from your export Azure storage container to [Event Hubs](https://azure.microsoft.com/en-us/services/event-hubs/) and to another Azure storage container.
This sample is located under `export-streaming`.
This sample is a console application that creates an observable that parses exported data, and multicasts the observable data into both an Event Hub and an Azure storage container.

To build the sample:
```
dotnet build export-streaming.sln
```