


namespace ElasticBulkInsertConsoleApp
{
    public class LogEntryModel
    {
        public Guid Guid { get; set; }
        public DateTime DateUtc { get; set; }
        public string Message { get; set; }
        public string Exception { get; set; }
    }
}



