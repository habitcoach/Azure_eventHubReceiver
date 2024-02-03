using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

namespace _02_ReceiveEvents
{
    class Program
    {

        private const string connectionString = "Endpoint=sb://ns166174eventhub02.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=APmYgyge1r0scCfmsfIzbjJhSuHgQ68hR+AEhPJhdr4=";
        private const string eventHubName = "eventhub01";

        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=166174sa03;AccountKey=pV5jvBKeyPRj2cuZ6JLOyHMTa3ZAl1DEDoMzEgpHUjSvyLhNXIuCrVII5F5xj9kIvmn5BeBS/fKN+AStYgExRg==;EndpointSuffix=core.windows.net";
        private const string blobContainerName = "checkpoint";
        static async Task Main()
        {
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            // Create an event processor client to process events in the event hub
            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, connectionString, eventHubName);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            
                // Start the processing
                await processor.StartProcessingAsync();
           
            // Change this value as needed
            await Task.Delay(TimeSpan.FromSeconds(100));

            // Stop the processing
            await processor.StopProcessingAsync();
        }
        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            try
            {
                // Write the body of the event to the console window
                Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
               

                // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
                await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
            }catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}