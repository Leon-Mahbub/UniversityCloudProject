using Microsoft.Extensions.Configuration;
using MyCloudProject.Common;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Azure.Storage.Blobs;

using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;

namespace MyExperiment
{
    public class AzureStorageProvider : IStorageProvider
    {
        private MyConfig config;

        public AzureStorageProvider(IConfigurationSection configSection)
        {
            config = new MyConfig();
            configSection.Bind(config);
        }

        public async Task<string> DownloadInputFile(string fileName)
        {
            // return "../myinputfilexy.csv"
            
            // this.logger?.LogInformation("In function");
            BlobServiceClient client = new BlobServiceClient(config.StorageConnectionString);
            BlobContainerClient cclient = client.GetBlobContainerClient(config.TrainingContainer);
           
            
            var cloudStorageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(config.StorageConnectionString);
          
            Console.WriteLine(client.Uri.ToString());
            string basePath = Directory.GetCurrentDirectory();
            string destinationPath = Path.Combine(basePath,"..","..","..","..","SampleFiles");
            MakeDownloadSpaceEmpty(destinationPath);

            destinationPath = Path.Combine(destinationPath, config.TrainingContainer);

            if (!Directory.Exists(destinationPath))
            {
                Directory.CreateDirectory(destinationPath);
            }
           


            var x =  cclient.GetBlobs();
            int len = 0;
             foreach (var blobItem in x)
            {
                
                
               
                
                
                string url = $"{client.Uri.ToString()}{config.TrainingContainer}/{ blobItem.Name}";
                
                
                if (!Directory.Exists(destinationPath)) Directory.CreateDirectory(destinationPath);
                Console.WriteLine(destinationPath);
                // Download the public blob at https://aka.ms/bloburl
                Console.WriteLine(url);
                var blob = new CloudBlockBlob(new Uri(url),cloudStorageAccount.Credentials);
                string fileDest = Path.Combine(destinationPath, blobItem.Name);
                await blob.DownloadToFileAsync(fileDest, FileMode.Create);
                
               
                
            }
            Console.WriteLine(len);
           
            return destinationPath;
            

            
        }

        public async Task UploadExperimentResult(ExperimentResult result)
        {
            /// upload hamming distance between images
            /// sourceImageName, DestinationImagename, Distance
            /// 

            Console.WriteLine("uploadExperiment result");
            BlobServiceClient client = new BlobServiceClient(config.StorageConnectionString);

            
            string path = result.InputFileUrl;
            var directories = Directory.GetDirectories(path);
            foreach(var dir in directories)
            {
               // string subPath = Path.Combine(path, dir);

                var images = Directory.GetFiles(dir, "*", SearchOption.AllDirectories);
              
                
                if(images.Length != 0)
                {
                    String dirName = "image-group" + new DirectoryInfo(dir).Name;
                    int version = getSafeVersion(dirName, client);
                    string newContainerName = dirName + "-" + version;
                    Console.WriteLine($"path {dirName}");

                    BlobContainerClient container = await client.CreateBlobContainerAsync(newContainerName);
                   
                    for (int i = 0; i < images.Length; i++)
                    {
                        var fileN = Path.GetFileName(images[i]);
                        var imageBlob = container.GetBlobClient(fileN);
                        FileStream fileStream = System.IO.File.OpenRead(images[i]);
                        await imageBlob.UploadAsync(fileStream);

                    }

                }
            }


           
        }

        public async Task<byte[]> UploadResultFile(string fileName, byte[] data)
        {
            Console.WriteLine("in uploadResultFile");
            string filePath = Path.Combine(Path.Combine(Directory.GetCurrentDirectory(), config.ResultContainer),fileName);
            Console.WriteLine(filePath);

            var client = new BlobServiceClient(config.StorageConnectionString);
            var bclient = client.GetBlobContainerClient(config.ResultContainer);
            var bbclient = bclient.GetBlobClient(fileName);
            if (bbclient.Exists()) bbclient.Delete();

            var path = File.OpenRead(filePath);
            await bclient.UploadBlobAsync(fileName,path);

            return Encoding.ASCII.GetBytes(client.Uri.ToString());
        }


        void MakeDownloadSpaceEmpty(string path)
        {
            if (Directory.Exists(path))
                SchemaImageClassification.DeleteDirectory(path);
            else Directory.CreateDirectory(path);
        }

        int getSafeVersion(string blobBaseName,BlobServiceClient client)
        {
            int version = 0;
            string name0 = blobBaseName + "-" + "0";
            var _client = client.GetBlobContainerClient(name0);
            if(_client.Exists())
            {
                _client.DeleteIfExists();
                return 1;
            }
            else
            {
                string name1 = blobBaseName + "-" + "1";
                 _client = client.GetBlobContainerClient(name1);
                if (_client.Exists())
                {
                    _client.DeleteIfExists();
                    return 0;
                }
                else return 0;
            }

        }
    }

    
}
