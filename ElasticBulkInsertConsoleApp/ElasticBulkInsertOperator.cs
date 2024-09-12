using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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

        #region Metriksiz
        public async Task SaveLogSingle(List<LogEntryModel> errorLogList)
        {
            foreach (var logEntryModel in errorLogList)
            {
                await _elasticClient.IndexAsync(logEntryModel, p => p.Index($"my_logs_{logEntryModel.DateUtc.ToString("yyyyMMdd")}"));
            }
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
        }
        #endregion





        #region 1. Zaman Ölçme (Execution Time)

        // Tek tek insert için zaman ölçümü
        public async Task<long> SaveLogSingleWithMetrics(List<LogEntryModel> errorLogList)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            foreach (var logEntryModel in errorLogList)
            {
                var result = await _elasticClient.IndexAsync(logEntryModel, p => p.Index($"my_logs_{logEntryModel.DateUtc.ToString("yyyyMMdd")}"));
            }

            stopwatch.Stop();
            Console.WriteLine($"Tek tek insert süresi: {stopwatch.ElapsedMilliseconds} ms");

            return stopwatch.ElapsedMilliseconds;
        }


        // Toplu insert için zaman ölçümü
        public async Task<long> SaveLogBulkWithMetrics(List<LogEntryModel> errorLogList)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            var bulkResponse = await _elasticClient.BulkAsync(b =>
                b.IndexMany(errorLogList, (p, logEntryModel) =>
                {
                    p.Index($"my_logs_{logEntryModel.DateUtc.ToString("yyyyMMdd")}")
                     .Id(logEntryModel.Guid);
                })
            );

            stopwatch.Stop();
            Console.WriteLine($"Toplu insert süresi: {stopwatch.ElapsedMilliseconds} ms");

            if (bulkResponse.Errors)
            {
                Console.WriteLine("Bulk insert had errors");
            }
            else
            {
                Console.WriteLine("Bulk insert completed successfully");
            }

            return stopwatch.ElapsedMilliseconds;
        }


        #endregion


        #region 2. Bellek Kullanımı (Memory Usage)

        public async Task<long> SaveLogSingleWithMemoryMetrics(List<LogEntryModel> errorLogList)
        {
            long initialMemory = GC.GetTotalMemory(true);

            foreach (var logEntryModel in errorLogList)
            {
                await _elasticClient.IndexAsync(logEntryModel, p => p.Index($"my_logs_{logEntryModel.DateUtc.ToString("yyyyMMdd")}"));
            }

            long finalMemory = GC.GetTotalMemory(true);
            long memoryUsage = finalMemory - initialMemory;
            Console.WriteLine($"Tek tek insert bellek kullanımı: {memoryUsage / 1024} KB");

            return memoryUsage;
        }


        public async Task<long> SaveLogBulkWithMemoryMetrics(List<LogEntryModel> errorLogList)
        {
            long initialMemory = GC.GetTotalMemory(true);

            var bulkResponse = await _elasticClient.BulkAsync(b =>
                b.IndexMany(errorLogList, (p, logEntryModel) =>
                {
                    p.Index($"my_logs_{logEntryModel.DateUtc.ToString("yyyyMMdd")}")
                     .Id(logEntryModel.Guid);
                })
            );

            long finalMemory = GC.GetTotalMemory(true);
            long memoryUsage = finalMemory - initialMemory;
            Console.WriteLine($"Toplu insert bellek kullanımı: {memoryUsage / 1024} KB");

            return memoryUsage;
        }


        #endregion
    }
}
