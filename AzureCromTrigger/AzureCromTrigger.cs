using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace AzureCromTrigger
{
    public static class ATTDataTransfer
    {
        [FunctionName("ATTDataTransfer")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            // This function will run every 5 minutes
            string server = "<sql server address>";
            string database = "<database name>";
            string SQLServerConnectionString = String.Format("<sqlserver connection string>", server, database);
            string CSVpath = "<CSV file path>"; // CSV file Path
            string CSVFileConnectionString = String.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};;Extended Properties=\"text;HDR=Yes;FMT=Delimited\";", CSVpath);
            var AllFiles = new DirectoryInfo(CSVpath).GetFiles("*.CSV");
            foreach (var file in AllFiles)
            {
                try
                {
                    DataTable dt = new DataTable();
                    using (OleDbConnection con = new OleDbConnection(CSVFileConnectionString))
                    {
                        con.Open();
                        var csvQuery = string.Format("select * from [{0}]", file.Name);
                        using (OleDbDataAdapter da = new OleDbDataAdapter(csvQuery, con))
                        {
                            da.Fill(dt);
                        }
                    }

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(SQLServerConnectionString))
                    {                    
                        // Change table name here
                        bulkCopy.DestinationTableName = "<Destination sql table name>";
                        bulkCopy.BatchSize = 0;
                        bulkCopy.WriteToServer(dt);
                        bulkCopy.Close();
                    }
                }
                catch (Exception ex)
                {
                    log.Error("An exception occured.", ex);
                }
            }
        }
    }
}