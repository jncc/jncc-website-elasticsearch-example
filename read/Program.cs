using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Aws4RequestSigner;
using dotenv.net;
using Newtonsoft.Json;

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

            var q = new {
                _source = new { excludes =  new [] { "content" } },
                from = start,
                size = size,
                query =  new {
                    @bool = new {
                        filter = new [] {
                            new { match = new { site = site } }
                        },
                        must = new [] {
                            new {
                                common = new {
                                    content = new {
                                        query = query,
                                        cutoff_frequency = 0.001,
                                        low_freq_operator = "or"
                                    }
                                }
                            }
                        },
                        should = new [] {
                            new {
                                common = new {
                                    title = new {
                                        query = query,
                                        cutoff_frequency = 0.001,
                                        low_freq_operator = "or"
                                    }
                                }
                            }
                        }
                    }
                },
                highlight = new {
                    fields = new { content = new {} }
                }
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = new Uri(Env.Var.ESEndpoint + "test/_search"),
                Content = new StringContent(
                    JsonConvert.SerializeObject(q, Formatting.None),
                    Encoding.UTF8,
                    "application/json"
                )
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