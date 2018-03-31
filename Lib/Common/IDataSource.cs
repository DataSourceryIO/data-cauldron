namespace DataSourcery.Data
{
    using System;
    using System.Threading.Tasks;

    public interface IDataSource
    {
        IDataSource SetDataCauldron(Uri dataCauldronUri);

        IDataSource LoadRecipe(string recipeName);

        Task<int> InvokeRecipe();
    }
}