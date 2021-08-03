using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MyCloudProject.Common;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MyExperiment
{
    public class Experiment : IExperiment
    {
        private IStorageProvider storageProvider;

        private ILogger logger;

        private MyConfig config;

        public Experiment(IConfigurationSection configSection, IStorageProvider storageProvider, ILogger log)
        {
            this.storageProvider = storageProvider;
            this.logger = log;

            config = new MyConfig();
            configSection.Bind(config);
        }

        public Task<ExperimentResult> Run(string inputFile)
        {
            // TODO read file

            ExperimentResult res = new ExperimentResult(this.config.GroupId, null);

            res.StartTimeUtc = DateTime.UtcNow;

            // Run your experiment code here.

            return Task.FromResult< ExperimentResult>(res); // TODO...
        }

        /// <inheritdoc/>
        public async Task RunQueueListener(CancellationToken cancelToken)
        {
            CloudQueue queue = await CreateQueueAsync(config);
            Console.WriteLine("queue created");
            

            while (cancelToken.IsCancellationRequested == false)
            {
                CloudQueueMessage message = await queue.GetMessageAsync();
                Console.WriteLine($"getting message { message!=null}");
                if (message != null)
                {
                    try
                    {
                        this.logger?.LogInformation($"Received the message {message.AsString}");

                        // TODO...
                        // ExerimentRequestMessage = deserialize it from msg. See WIKI.
                        ExerimentRequestMessage msg = null;

                        var inputFile = await this.storageProvider.DownloadInputFile("");
                        Console.WriteLine(inputFile);
                        string outputImagePath = SchemaImageClassification.RunSEProject(inputFile,"resultxl",outputFileName:config.ResultXL);

                        ExperimentResult result = new ExperimentResult("","");
                        result.InputFileUrl = outputImagePath;
                        
                        //TODO. do serialization of the result.
                        await storageProvider.UploadResultFile(config.ResultXL,null);

                        await storageProvider.UploadExperimentResult(result);

                        await queue.DeleteMessageAsync(message);
                    }
                    catch (Exception ex)
                    {
                        this.logger?.LogError(ex, "TODO...");
                    }
                }
                else
                    await Task.Delay(500);
            }

            this.logger?.LogInformation("Cancel pressed. Exiting the listener loop.");
        }


        #region Private Methods


        /// <summary>
        /// Validate the connection string information in app.config and throws an exception if it looks like 
        /// the user hasn't updated this to valid values. 
        /// </summary>
        /// <param name="storageConnectionString">The storage connection string</param>
        /// <returns>CloudStorageAccount object</returns>
        private static CloudStorageAccount CreateStorageAccountFromConnectionString(string storageConnectionString)
        {
            CloudStorageAccount storageAccount;
            try
            {
                storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            }
            catch (FormatException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
                Console.ReadLine();
                throw;
            }
            catch (ArgumentException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            return storageAccount;
        }

        /// <summary>
        /// Create a queue for the sample application to process messages in. 
        /// </summary>
        /// <returns>A CloudQueue object</returns>
        private static async Task<CloudQueue> CreateQueueAsync(MyConfig config)
        {
            // Retrieve storage account information from connection string.
            CloudStorageAccount storageAccount = CreateStorageAccountFromConnectionString(config.StorageConnectionString);

            // Create a queue client for interacting with the queue service
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            Console.WriteLine("1. Create a queue for the demo");

            CloudQueue queue = queueClient.GetQueueReference(config.Queue);
            try
            {
                Console.WriteLine("trying to create queue");
                await queue.CreateIfNotExistsAsync();
                Console.WriteLine("trying to create queue 2");
            }
            catch
            {
                Console.WriteLine("If you are running with the default configuration please make sure you have started the storage emulator.  ess the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            return queue;
        }
        #endregion
    }
}
