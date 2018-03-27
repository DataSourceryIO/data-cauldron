using Microsoft.WindowsAzure.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Collections.Generic;
using DataSourcery.Data.SqlServer;

namespace DataSourcery.Data.Azure.Test
{
    [TestClass]
    public class AzureDataCauldronUnitTests
    {
        static private string sqlConnectionString = @"Initial Catalog=AdventureWorks2017;Server=.\MSSQLSERVER01;Integrated Security=True;";
        static private string azureStorageConnectionString = "UseDevelopmentStorage=true";
        static private CloudStorageAccount storageAccount;
        static private SqlConnection sqlConnection;
        static private CloudBlobContainer configContainer;
        static private CloudBlobContainer extractContainer;

        [ClassInitialize]
        static public async Task ClassInitialize(TestContext testContext)
        {
            storageAccount = CloudStorageAccount.Parse(azureStorageConnectionString);
            sqlConnection = new SqlConnection(sqlConnectionString);

            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            configContainer = cloudBlobClient.GetContainerReference("azuredatacauldronunittestsconfig");
            await configContainer.CreateIfNotExistsAsync();

            extractContainer = cloudBlobClient.GetContainerReference("azuredatacauldronunittestsextract");
            await extractContainer.CreateIfNotExistsAsync();

            CloudBlockBlob cloudBlockBlob = configContainer.GetBlockBlobReference(
                Path.Combine(@"SqlServerDatasource\AdventureWorks2017\TransactionHistory", @"SinglePageTest.sql"));
            cloudBlockBlob.UploadFromFileAsync(
                Path.Combine(Directory.GetCurrentDirectory() + @"\Source", @"SinglePageTest.sql")).Wait();
        }

        [ClassCleanup]
        static public void ClassCleanup()
        {
            sqlConnection.Dispose();
        }

        [TestInitialize]
        public void TestInitialize()
        {

        }

        [TestCleanup]
        public void TestCleanup()
        {

        }       

        [TestMethod]
        public void TestConfigureDataSources()
        {
            var dc = new AzureDataCauldron()
                .SetConfigurationContainer(configContainer)
                .SetExtractContainer(extractContainer);

            AsyncHelpers.RunSync(() => dc.RegisterDataSources(typeof(SqlServerDataSource), @"\.sql$"));

            Assert.AreNotEqual(0, dc.Registry.Count);
        }

        [TestMethod]
        public void TestCaptureChange()
        {
            var dc = new AzureDataCauldron()
                .SetConfigurationContainer(configContainer)
                .SetExtractContainer(extractContainer);

            AsyncHelpers.RunSync(() => dc.RegisterDataSources(typeof(SqlServerDataSource), @"\.sql$"));

            int result = 0;
            foreach (KeyValuePair<string, IDataSource> registryEntry in dc.Registry)
            {
                ((SqlServerDataSource)registryEntry.Value).SetConnection(sqlConnection);
                result += AsyncHelpers.RunSync<int>(() => registryEntry.Value.Extract());
            }

            Assert.AreNotEqual(0, result);
        }
    }
}
