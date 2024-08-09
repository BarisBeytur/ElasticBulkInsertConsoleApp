using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticBulkInsertConsoleApp
{
    public class ElasticBulkInsertOperator
    {
        private readonly ElasticsearchClient _elasticClient;

        public ElasticBulkInsertOperator()
        {
            var settings = new ElasticsearchClientSettings(new Uri("https://localhost:9200")) // Elasticsearch default port: 9200
                .Authentication(new BasicAuthentication("elastic", "J0=jcRjdUKB3rS21tYao"))
                .ServerCertificateValidationCallback((sender, certificate, chain, sslPolicyErrors) => true) // SSL bypass
                .DefaultIndex("my_logs_*")
                .DefaultFieldNameInferrer(p => p.ToLower()); // Convert field names to lower case

            _elasticClient = new ElasticsearchClient(settings);
        }

        public async Task SaveLogBulk(List<LogEntryModel> errorLogList)
        {
            var bulkResponse = await _elasticClient.BulkAsync(b =>
                b.IndexMany(errorLogList, (p, logEntryModel) =>
                {
                    p.Index($"my_logs_{logEntryModel.DateUtc.ToString("yyyyMMdd")}") // my_logs_20240101
                     .Id(logEntryModel.Guid);
                })
            );

            if (bulkResponse.Errors)
            {
                Console.WriteLine("Bulk insert had errors");
            }
            else
            {
                Console.WriteLine("Bulk insert completed successfully");
            }
        }
    }
}
