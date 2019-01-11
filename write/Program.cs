using System;
using System.IO;
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
            var credentials = new BasicAWSCredentials(Env.Var.AwsAccessKey, Env.Var.AwsSecretAccessKey);
            var region = RegionEndpoint.GetBySystemName(Env.Var.AwsRegion);
            var s3 = new AmazonS3Client(credentials, region);
            var sqs = new AmazonSQSClient(credentials, region);
            var sqsExtendedClient = new AmazonSQSExtendedClient(sqs,
                new ExtendedClientConfiguration().WithLargePayloadSupportEnabled(s3, Env.Var.SqsPayloadBucket)
            );

            // index a simple document
            
            var simpleMessage = new
            {
                verb = "upsert",
                index = "test",
                document = new
                {
                    id = "123456789", // ID managed by Umbraco
                    site = "website", // as opposed to datahub|sac|mhc
                    title = "An example searchable document",
                    content = "This is a searchable document made purely for example purposes.",
                    url = "http://example.com/pages/123456789", // the URL of the page, for clicking through
                    keywords = new []
                    {
                        new { vocab = "http://vocab.jncc.gov.uk/jncc-web", value = "Example" }
                    },
                    published_date = "2019-01-14",
                }
            };

            var basicResponse = await sqsExtendedClient.SendMessageAsync(Env.Var.SqsEndpoint,
                JsonConvert.SerializeObject(simpleMessage, Formatting.None)
            );

            Console.WriteLine(basicResponse.MessageId);

            // index a PDF
            
            var pdf = File.ReadAllBytes(@"./OffshoreBrighton_SACO_V1.0.pdf");
            var pdfEncoded = Convert.ToBase64String(pdf);

            var pdfMessage = new
            {
                verb = "upsert",
                index = "test",
                document = new
                {
                    id = "987654321",
                    site = "website", 
                    title = "An example PDF document",
                    content = "This is a PDF document searchable purely for example purposes.",
                    url = "http://example.com/pages/987654321",
                    keywords = new []
                    {
                        new { vocab = "http://vocab.jncc.gov.uk/jncc-web", value = "Example" }
                    },
                    published_date = "2019-01-14",
                    content_base64 = pdfEncoded, // base-64 encoded content when this is a PDF, etc.
                    mime_type = "application/pdf", // only needed when e.g. a PDF
                }
            };

            var pdfResponse = await sqsExtendedClient.SendMessageAsync(Env.Var.SqsEndpoint,
                JsonConvert.SerializeObject(pdfMessage, Formatting.None)
            );

            Console.WriteLine(pdfResponse.MessageId);
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
