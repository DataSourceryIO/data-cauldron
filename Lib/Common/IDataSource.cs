namespace DataSourcery.Data
{
    using System;
    using System.Threading.Tasks;

    public interface IDataSource
    {
        IDataSource SetConfiguration(string configuration);

        IDataSource SetPostExtractAction(Action<IDataSource, string> postExtractAction);

        string Watermark { get; set; }

        Task<int> Extract();
    }
}