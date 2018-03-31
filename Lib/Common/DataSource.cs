namespace DataSourcery.Data
{
    using System;
    using System.Threading.Tasks;

    public class DataSource : IDataSource
    {
        public Uri DataCauldronUri { get; private set; }

        public Recipe Recipe { get; private set; }

        public IDataSource SetDataCauldron(Uri dataCauldronUri)
        {
            this.DataCauldronUri = dataCauldronUri;
            return this;
        }

        public IDataSource LoadRecipe(string recipeName)
        {
            Recipe = DataServiceController.GetRecipe(DataCauldronUri, recipeName).GetAwaiter().GetResult();
            return this;
        }

        public virtual Task<int> InvokeRecipe()
        {
            throw new NotImplementedException();
        }

        protected async Task PourDataIn(string data, string watermark)
        {
            string newWatermark = await DataServiceController.PourDataIn(DataCauldronUri, Recipe.Name, data, watermark);
        }
    }
}