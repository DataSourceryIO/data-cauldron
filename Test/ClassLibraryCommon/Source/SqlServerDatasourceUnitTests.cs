using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data.SqlClient;
using System;
using System.Threading.Tasks;
using System.IO;

namespace DataSourcery.Data.SqlServer.Test
{
    [TestClass]
    public class UnitTest1
    {
        private string connectionString = @"Initial Catalog=AdventureWorks2017;Server=.\MSSQLSERVER01;Integrated Security=True;";
        private Uri localDataCauldronUri = new Uri(Directory.GetCurrentDirectory());

        [TestMethod]
        public void TestSetDataCauldron()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource();
                ds.SetDataCauldron(localDataCauldronUri);

                Assert.IsNotNull(ds.DataCauldronUri);
                Assert.IsTrue(ds.DataCauldronUri.IsFile);
            }
        }

        [TestMethod]
        public void TestLoadRecipeLocal()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource();
                ds.SetDataCauldron(localDataCauldronUri);
                ds.LoadRecipe(@"Source\TestBasicRead.sql");

                Assert.IsNotNull(ds.Recipe);
                Assert.IsNotNull(ds.Recipe.Implementation);
            }
        }

        [TestMethod]
        public void TestRead()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource();
                ds.SetConnection(connection);
                ds.SetDataCauldron(localDataCauldronUri);
                ds.LoadRecipe(@"Source\TestBasicRead.sql");

                Assert.IsNotNull(ds.Recipe);

                int result = ds.InvokeRecipe().GetAwaiter().GetResult();

                Assert.AreNotEqual(0, result);
            }
        }

        [TestMethod]
        public void TestWatermarkRead()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource();
                ds.SetConnection(connection);
                ds.SetDataCauldron(localDataCauldronUri);
                ds.LoadRecipe(@"Source\TestWatermarkRead.sql");

                Assert.IsNotNull(ds.Recipe);

                int result = ds.InvokeRecipe().GetAwaiter().GetResult();

                Assert.AreNotEqual(0, result);
            }
        }
    }
}
