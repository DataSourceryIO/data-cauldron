namespace DataSourcery.Data.Azure
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System.Text.RegularExpressions;

    public class AzureDataCauldron : IDatacauldron
    {
        private const string data_file_format = @"{0}-Extract-{1}.json";
        private const string watermark_metadata_key = "Watermark";

        private CloudBlobContainer configContainer;
        private CloudBlobContainer extractContainer;

        public Dictionary<String, IDataSource> Registry { get; }

        public AzureDataCauldron()
        {
            Registry = new Dictionary<String, IDataSource>();
        }

        public IDatacauldron SetConfigurationContainer(object container)
        {
            configContainer = (CloudBlobContainer)container;
            return this;
        }

        public IDatacauldron SetExtractContainer(object container)
        {
            extractContainer = (CloudBlobContainer)container;
            return this;
        }

        public async Task RegisterDataSources(Type dataSourceType, string dataSoureTypeConfigurationFileNamePattern)
        {
            foreach(KeyValuePair<string, IDataSource> registryEntry in Registry)
                if (registryEntry.Value.GetType() == dataSourceType.GetType())
                    Registry.Remove(registryEntry.Key);

            BlobContinuationToken continuationToken = null;
            BlobResultSegment resultSegment = null;

            do
            {
                Regex regex = new Regex(dataSoureTypeConfigurationFileNamePattern, RegexOptions.IgnoreCase);

                resultSegment = await configContainer.ListBlobsSegmentedAsync(
                    "", true, BlobListingDetails.None | BlobListingDetails.Metadata, 100, continuationToken, null, null);
                foreach (var blobItem in resultSegment.Results)
                {
                    CloudBlockBlob blockBlob = (CloudBlockBlob)blobItem;
                    if (regex.Match(blockBlob.Name).Success)
                    {
                        IDataSource ds = ((IDataSource)Activator.CreateInstance(dataSourceType))
                            .SetConfiguration(await blockBlob.DownloadTextAsync())
                            .SetPostExtractAction(UploadDataExtract);

                        if (blockBlob.Metadata.ContainsKey(watermark_metadata_key))
                            ds.Watermark = blockBlob.Metadata[watermark_metadata_key];

                        Registry[blockBlob.Name] = ds;
                    }
                }

                continuationToken = resultSegment.ContinuationToken;
            }
            while (continuationToken != null);
        }

        private CloudBlockBlob GetConfigurationBlockBlob(IDataSource datasource)
        {
            CloudBlockBlob result = null;
            foreach (KeyValuePair<string, IDataSource> registryEntry in Registry)
            {
                if (registryEntry.Value == datasource)
                {
                    result = configContainer.GetBlockBlobReference(registryEntry.Key);
                    break;
                }
            }
            return result;
        }

        private void UploadDataExtract(IDataSource datasource, string page)
        {
            CloudBlockBlob configurationBlockBlob = GetConfigurationBlockBlob(datasource);

            if (configurationBlockBlob != null)
            {
                string pageFileName = String.Format(data_file_format, configurationBlockBlob.Name, Guid.NewGuid());
                CloudBlockBlob dataPageBlockBlob = extractContainer.GetBlockBlobReference(pageFileName);

                byte[] byteArray = Encoding.UTF8.GetBytes(page);
                MemoryStream stream = new MemoryStream(byteArray);

                dataPageBlockBlob.UploadFromStreamAsync(stream).Wait();

                if (datasource.Watermark != null)
                    configurationBlockBlob.Metadata[watermark_metadata_key] = datasource.Watermark;

                configurationBlockBlob.SetMetadataAsync().Wait();
            }
        }
    }
}
