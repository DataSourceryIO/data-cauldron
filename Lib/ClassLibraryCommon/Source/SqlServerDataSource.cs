namespace DataSourcery.Data.SqlServer
{
    using System;
    using System.Threading.Tasks;
    using System.Data.SqlClient;
    using System.Collections.Generic;
    using Newtonsoft.Json;
    using System.Text.RegularExpressions;

    public class SqlServerDataSource : IDataSource
    {
        private const string watermark_pattern = @"{[\W]*watermark[\W]*,[\W]*([\S]+)[\W]*,[\W]*([\S]+)[\W]*}";
        private string watermarkKeyName = null;

        private int retryCount = 3;
        private readonly TimeSpan retryDelay = TimeSpan.FromSeconds(60);

        private string _configuration = null;
        private Action<IDataSource, string> _postExtractAction = null;
        private SqlConnection _connection;

        public string Watermark { get; set; }
        public int PageSize { get; set; } = 1000;

        public IDataSource SetConfiguration(string configuration)
        {
            _configuration = configuration;
            return this;
        }

        public IDataSource SetPostExtractAction(Action<IDataSource, string> postExtractAction)
        {
            _postExtractAction = postExtractAction;
            return this;
        }

        public IDataSource SetConnection(SqlConnection connection)
        {
            _connection = connection;
            return this;
        }

        public async Task<int> Extract()
        {
            string queryString = _configuration;

            Regex regex = new Regex(watermark_pattern, RegexOptions.IgnoreCase);
            Match match = regex.Match(_configuration);
            if (match.Success)
            {
                watermarkKeyName = match.Groups[1].Value;
                string defaultWatermarkValue = match.Groups[2].Value;
                queryString = Regex.Replace(queryString, watermark_pattern, 
                    String.IsNullOrEmpty(Watermark) ? defaultWatermarkValue : Watermark, RegexOptions.IgnoreCase);
            }
            return await ExecuteSQLWithBasicRetryAsync(queryString);
        }



        async private Task<int> ExecuteSQLWithBasicRetryAsync(string sql)
        {
            int currentRetry = 0;

            for (; ; )
            {
                try
                {
                    return await ExecuteSQL(sql);
                }
                catch (Exception ex)
                {

                    currentRetry++;

                    if (currentRetry > this.retryCount || !IsTransient(ex))
                    {
                        throw;
                    }
                }

                await Task.Delay(retryDelay);
            }
        }

        private bool IsTransient(Exception exception)
        {
            if (exception is System.Data.SqlClient.SqlException)
            {
                System.Data.SqlClient.SqlException exSqlException = (System.Data.SqlClient.SqlException)exception;
                for (int i = 0; i < exSqlException.Errors.Count; i++)
                {
                    // Cannot connect to server
                    if (exSqlException.Errors[i].Number == 2) return true;
                }
            }

            return false;
        }

        private async Task<int> ExecuteSQL(string queryString)
        {
            SqlCommand command = new SqlCommand(queryString, _connection)
            {
                CommandTimeout = 600
            };

            _connection.Open();

            SqlDataReader reader = await command.ExecuteReaderAsync();
            int result = Serialize(reader);

            _connection.Close();

            return result;
        }

        private int Serialize(SqlDataReader reader)
        {
            var page = new List<Dictionary<string, object>>();

            var cols = new List<string>();
            int watermark_col = -1;
            int rowcount = 0;

            for (var i = 0; i < reader.FieldCount; i++)
            {
                cols.Add(reader.GetName(i));
                watermark_col = reader.GetName(i) == watermarkKeyName ? i : watermark_col;
            }

            while (reader.Read())
            {
                rowcount++;
                if (rowcount % PageSize == 0)
                {
                    _postExtractAction?.Invoke(this, JsonConvert.SerializeObject(page, Formatting.None));
                    page = new List<Dictionary<string, object>>();
                }

                page.Add(SerializeRow(cols, reader));
                
                if (watermark_col >= 0)
                {
                    Watermark = reader[watermark_col].ToString();
                }
            }

            _postExtractAction?.Invoke(this, JsonConvert.SerializeObject(page, Formatting.None));

            return rowcount;
        }

        static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, SqlDataReader reader)
        {
            var result = new Dictionary<string, object>();

            foreach (var col in cols)
            {
                if (col.Contains("."))
                {
                    string objKey = col.Substring(0, col.IndexOf("."));
                    string objCol = col.Substring(col.IndexOf(".") + 1);
                    if (!result.ContainsKey(objKey))
                        result.Add(objKey, new Dictionary<string, object>());
                    Dictionary<string, object> objVal = (Dictionary<string, object>)result[objKey];
                    objVal.Add(objCol, reader[col]);
                }
                else
                    result.Add(col, reader[col]);
            }

            return result;
        }
    }
}
