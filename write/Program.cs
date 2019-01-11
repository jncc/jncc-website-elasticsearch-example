using System;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.Runtime;
using Amazon.SQS.ExtendedClient;
using Newtonsoft.Json;
using dotenv.net;

namespace write
{
    class Program
    {
        static void Main()
        {
            DotEnv.Config();
            Console.WriteLine("Hello World!");

            Example().GetAwaiter().GetResult();            
        }

        static async Task Example()
        {
            var s3Client = new AmazonS3Client(new BasicAWSCredentials(Env.Var.AwsAccessKey, Env.Var.AwsSecretAccessKey), RegionEndpoint.GetBySystemName(Env.Var.AwsRegion));
            var sqsClient = new AmazonSQSClient(new BasicAWSCredentials(Env.Var.AwsAccessKey, Env.Var.AwsSecretAccessKey), RegionEndpoint.GetBySystemName(Env.Var.AwsRegion));
            var sqsExtendedClient = new AmazonSQSExtendedClient(sqsClient, new ExtendedClientConfiguration().WithLargePayloadSupportEnabled(s3Client, Env.Var.SqsPayloadBucket));

            var message = new
            {
                verb = "upsert",
                index = "test",
                document = new
                {
                    id = "6f7fc96a-1120-41c8-a7fd-e320f92535cb",
                    site = "website",
                    title = "An example search entry",
                    content = "This is a search entry made pure for example purposes.",
                    keywords = new []
                    {
                        new { vocab = "http://vocab.jncc.gov.uk/jncc-web", value = "None" }
                    },
                    published_date = "2019-01-10",
                    // mime_type = "",
                    // data_type = "",
                    // data = "",
                }
            };

            string messageString = JsonConvert.SerializeObject(message, Formatting.None, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            Console.WriteLine(messageString);

            var result = await sqsExtendedClient.SendMessageAsync(Env.Var.SqsEndpoint, messageString);
            Console.WriteLine(result.MessageId);
        }
    }

    /// <summary>
    /// Provides environment variables.
    /// </summary>
    public class Env
    {    
        static Env() { } // singleton to avoid reading a variable more than once
        private static readonly Env env = new Env();

        public string AwsRegion          { get; private set; }
        public string AwsAccessKey       { get; private set; }
        public string AwsSecretAccessKey { get; private set; }
        public string SqsEndpoint        { get; private set; }
        public string SqsPayloadBucket   { get; private set; }
        
        private Env()
        {
            AwsRegion = GetVariable("AWS_REGION");
            AwsAccessKey = GetVariable("AWS_ACCESSKEY");
            AwsSecretAccessKey = GetVariable("AWS_SECRETACCESSKEY");
            SqsEndpoint= GetVariable("SQS_ENDPOINT");
            SqsPayloadBucket = GetVariable("SQS_PAYLOAD_BUCKET");
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
