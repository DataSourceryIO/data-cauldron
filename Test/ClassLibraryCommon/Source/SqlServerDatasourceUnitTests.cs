using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data.SqlClient;
using System;
using Newtonsoft.Json;

namespace DataSourcery.Data.SqlServer.Test
{
    [TestClass]
    public class UnitTest1
    {
        private string connectionString = @"Initial Catalog=AdventureWorks2017;Server=.\MSSQLSERVER01;Integrated Security=True;";

        [TestMethod]
        public void TestRead()
        {
            string queryString = @"SELECT TOP 100 TransactionID rk, ModifiedDate w, Quantity [p.Quantity] FROM Production.TransactionHistory";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource()
                    .SetConnection(connection)
                    .SetConfiguration(queryString);

                int result = AsyncHelpers.RunSync<int>(() => ds.Extract());

                Assert.AreNotEqual(0, result);
            }
        }

        [TestMethod]
        public void TestReadWithDateTimeWatermark()
        {
            string queryString = @"SELECT TOP 100 TransactionID rk, ModifiedDate w, Quantity [p.Quantity] FROM Production.TransactionHistory WHERE ModifiedDate > {Watermark, w, 2000-01-01}";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource()
                    .SetConnection(connection)
                    .SetConfiguration(queryString);
                ds.Watermark = DateTime.MinValue.ToShortDateString();

                int result = AsyncHelpers.RunSync<int>(() => ds.Extract());

                Assert.AreNotEqual(0, result);
                Assert.IsNotNull(ds.Watermark);
                Assert.AreNotEqual(DateTime.MinValue.ToShortDateString(), ds.Watermark);
            }
        }

        [TestMethod]
        public void TestReadWithDateTimeWatermarkPaged()
        {
            string queryString = @"SELECT TOP 100 TransactionID rk, ModifiedDate w, Quantity [p.Quantity] FROM Production.TransactionHistory WHERE ModifiedDate > {Watermark, w, 2000-01-01}";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource()
                    .SetConnection(connection)
                    .SetConfiguration(queryString);
                ds.Watermark = DateTime.MinValue.ToShortDateString();

                int result = AsyncHelpers.RunSync<int>(() => ds.Extract());

                Assert.AreNotEqual(0, result);
                Assert.IsNotNull(ds.Watermark);
                Assert.AreNotEqual(DateTime.MinValue.ToShortDateString(), ds.Watermark);
            }
        }

        [TestMethod]
        public void TestReadWithInt32Watermark()
        {
            string queryString = @"SELECT TOP 100 TransactionID rk, TransactionID w, Quantity [p.Quantity] FROM Production.TransactionHistory WHERE TransactionID > {Watermark, w, 0}";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                var ds = new SqlServerDataSource()
                    .SetConnection(connection)
                    .SetConfiguration(queryString);
                ds.Watermark = Int32.MinValue.ToString();

                int result = AsyncHelpers.RunSync<int>(() => ds.Extract());

                Assert.AreNotEqual(0, result);
                Assert.IsNotNull(ds.Watermark);
                Assert.AreNotEqual(Int32.MinValue.ToString(), ds.Watermark);
            }
        }

    }
}
