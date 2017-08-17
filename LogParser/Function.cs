using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using LogParser.Model;
using Newtonsoft.Json;

namespace LogParser {
    
    //--- Classes ---
    public class Function {
    
        //--- Fields ---
        private readonly string logsBucket = Environment.GetEnvironmentVariable("LOGS_BUCKET");
        private readonly IAmazonS3 _s3Client;
        
        //--- Constructors ---
        public Function() {
            _s3Client = new AmazonS3Client();
        }

        //--- Methods ---
        [LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
        public void Handler(CloudWatchLogsEvent cloudWatchLogsEvent, ILambdaContext context) {
            
            // Level 1: decode and decompress data
            var logsData = cloudWatchLogsEvent.AwsLogs.Data;
            Console.WriteLine($"THIS IS THE DATA: {logsData}");
            var decompressedData = DecompressLogData(logsData);
            Console.WriteLine($"THIS IS THE DECODED, UNCOMPRESSED DATA: {decompressedData}");
            
            // Level 2: Parse log records
            var athenaFriendlyJson = ParseLog(decompressedData);
            // Level 3: Save data to S3
            PutObject(athenaFriendlyJson);

            // Level 4: Create athena schema to query data
        }
        
        public static string DecompressLogData(string value) {
            var bytesZipFile = Convert.FromBase64String(value);
            using (Stream originalFileStream = new MemoryStream(bytesZipFile))
            {
                using (GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress))
                using (var sr = new StreamReader(decompressionStream)) 
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static IEnumerable<string> ParseLog(string data) 
        {
            var decompressedEvent = JsonConvert.DeserializeObject<DecompressedEvents>(data);
            LambdaLogger.Log($"Deserialized the event with {decompressedEvent.LogEvents.Count()} events");
            foreach(var logEvent in decompressedEvent.LogEvents)
            {
                var messageParts = logEvent.Message.Split('λ');
                var dict = new Dictionary<string, string>();
                LambdaLogger.Log($"There are {messageParts.Count()} message parts");

                foreach(var part in messageParts)
                {
                    var fooSplit = Regex.Split(part, @"\[(.*?)\]");
                    var key = fooSplit[1];
                    LambdaLogger.Log($"fooSplit Count: {fooSplit.Count()}");
                    if(dict.ContainsKey(key))
                        LambdaLogger.Log($"The dictionary already contains the key: {key}");
                    else
                        dict.Add(fooSplit[1].ToLower(), fooSplit[2].Substring(1).Trim());
                }

                yield return JsonConvert.SerializeObject(dict);
            }
        }

        public void PutObject(IEnumerable<string> values) {
            _s3Client.PutObjectAsync(new PutObjectRequest(){
                BucketName = "team4-lambda-sharp-s3-logs",
                Key = Guid.NewGuid().ToString(),
                ContentBody = string.Join("\n", values)
            }).Wait();
        }
    }
}
