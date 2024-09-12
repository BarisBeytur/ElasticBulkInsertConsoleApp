using System;
using System.Globalization;


namespace ElasticBulkInsertConsoleApp
{
    class Program
    {

        public static List<LogEntryModel> logs;

        public static List<LogEntryModel> GetErrorLogs(DateTime startDate, DateTime endDate)
        {
            var logEntryModels = new List<LogEntryModel>();

            for (var date = startDate; date < endDate; date = date.AddSeconds(30))
            {
                var logEntryModel = new LogEntryModel
                {
                    DateUtc = date,
                    Guid = Guid.NewGuid(),
                    Message = $"Error message {date}",
                    Exception = $"Exception {date}",
                };

                logEntryModels.Add(logEntryModel);
            }

            return logEntryModels;
        }



        static async Task Main(string[] args)
        {
            ElasticBulkInsertOperator elasticBulkInsertOperator = new ElasticBulkInsertOperator();

            logs = GetErrorLogs(DateTime.ParseExact("01.01.2024", "dd.MM.yyyy", CultureInfo.InvariantCulture), DateTime.ParseExact("04.01.2024", "dd.MM.yyyy", CultureInfo.InvariantCulture));

            Console.WriteLine($"Bulk insert starting for {logs.Count}");

            // Metriksiz çalıştırma
            await elasticBulkInsertOperator.SaveLogSingle(logs);
            await elasticBulkInsertOperator.SaveLogBulk(logs);

            // Zaman metrikli çalıştırma
            await elasticBulkInsertOperator.SaveLogSingleWithMetrics(logs);
            await elasticBulkInsertOperator.SaveLogBulkWithMetrics(logs);

            // Bellek metrikli çalıştırma
            await elasticBulkInsertOperator.SaveLogSingleWithMemoryMetrics(logs);
            await elasticBulkInsertOperator.SaveLogBulkWithMemoryMetrics(logs);

        }
    }
}




