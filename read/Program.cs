using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Aws4RequestSigner;
using dotenv.net;

namespace jncc_es_sample
{
    class Program
    {
        static void Main()
        {
            DotEnv.Config();
            Console.WriteLine(Env.Var.ESEndpoint);

            Example().GetAwaiter().GetResult();
        }

        static async Task Example()
        {
            int start = 0;
            int size = 10;
            string site = "datahub";
            string query = "habitats";

            string q = String.Format(@"{{
                ""_source"": {{ ""excludes"": [ ""content"" ] }},
                ""from"": {0},
                ""size"": {1},
                ""query"": {{
                    ""bool"": {{
                        ""filter"": [
                            {{ ""match"": {{ ""site"": ""{2}"" }} }}
                        ],
                        ""must"": [
                            {{ ""common"": {{ ""content"": {{ ""query"": ""{3}"", ""cutoff_frequency"": 0.001, ""low_freq_operator"": ""or"" }} }} }}
                        ],
                        ""should"": [
                            {{ ""common"": {{ ""title"": {{ ""query"": ""{3}"", ""cutoff_frequency"": 0.001, ""low_freq_operator"": ""or"" }} }} }}
                        ]
                    }}
                }},
                ""highlight"": {{
                    ""fields"": {{ ""content"": {{}} }}
                }}
            }}", start, size, site, query).Trim();

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri(Env.Var.ESEndpoint + "test/_search"),
                Content = new StringContent(q, Encoding.UTF8, "application/json")
            };

            var signedRequest = await GetSignedRequest(request);
            var response = await new HttpClient().SendAsync(signedRequest);
            var responseString = await response.Content.ReadAsStringAsync();        

            Console.WriteLine(responseString);
        }

        static async Task<HttpRequestMessage> GetSignedRequest(HttpRequestMessage request)
        {
            var signer = new AWS4RequestSigner(Env.Var.ESAwsAccessKey, Env.Var.ESAwsSecretAccessKey);
            return await signer.Sign(request, "es", Env.Var.ESAwsRegion);
        }
    }


    /// <summary>
    /// Provides environment variables.
    /// </summary>
    public class Env
    {    
        static Env() { } // singleton to avoid reading a variable more than once
        private static readonly Env env = new Env();

        public string ESAwsRegion          { get; private set; }
        public string ESAwsAccessKey       { get; private set; }
        public string ESAwsSecretAccessKey { get; private set; }
        public string ESEndpoint           { get; private set; }
        
        private Env()
        {
            ESAwsRegion = GetVariable("ELASTICSEARCH_AWS_REGION");
            ESAwsAccessKey = GetVariable("ELASTICSEARCH_AWS_ACCESSKEY");
            ESAwsSecretAccessKey = GetVariable("ELASTICSEARCH_AWS_SECRETACCESSKEY");
            ESEndpoint = GetVariable("ELASTICSEARCH_ENDPOINT");
        }

        string GetVariable(string variable)
        {
            return Environment.GetEnvironmentVariable(variable)
                ?? throw new Exception($"The environment variable {variable} couldn't be read. You may need to define it in your .env file.");
        }

        public static Env Var
        {
            get { return env; }
        }
    }

}