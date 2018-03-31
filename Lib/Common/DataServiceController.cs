namespace DataSourcery.Data
{
    using System;
    using System.IO;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Runtime.Serialization.Json;
    using System.Text;
    using System.Threading.Tasks;

    public class Recipe
    {
        public string Name;
        public string Implementation;
        public string Watermark;
    }

    public sealed class DataServiceController
    {
        private static readonly DataServiceController instance = new DataServiceController();
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly DataContractJsonSerializer recipeSerializer = new DataContractJsonSerializer(typeof(Recipe));

        private DataServiceController() { }

        public static DataServiceController Instance
        {
            get
            {
                httpClient.DefaultRequestHeaders.Accept.Clear();
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                httpClient.DefaultRequestHeaders.Add("User-Agent", "DataSourcery.DataSource client");

                return instance;
            }
        }

        public static async Task<Recipe> GetRecipe(Uri dataCauldronUri, string recipeName)
        {
            if (dataCauldronUri.IsFile)
            {
                return new Recipe
                {
                    Name = recipeName,
                    Implementation = File.ReadAllText(Path.Combine(dataCauldronUri.LocalPath, recipeName))
                };
            }
            else
            {
                var streamTask = httpClient.GetStreamAsync(dataCauldronUri + "/api/GetRecipe?name=" + recipeName);
                return recipeSerializer.ReadObject(await streamTask) as Recipe;
            }
        }

        public static async Task<string> PourDataIn(Uri dataCauldronUri, string recipeName, string data, string watermark)
        {
            if (dataCauldronUri.IsFile)
            {
                File.WriteAllText(Path.Combine(dataCauldronUri.LocalPath, String.Format(@"{0}-Extract-{1}.json", recipeName, Guid.NewGuid())), data);
            }
            else
            {
                var httpContent = new StringContent(data, Encoding.UTF8, "application/json");
                var httpResponse = await httpClient.PostAsync(String.Format("{0}/api/PourDataIn?name={1}&watermark={2}", dataCauldronUri, recipeName, watermark), httpContent);
            }

            return watermark;
        }
    }
}
