
using System;
using System.Globalization;
using System.IO;
using QuantConnect.Configuration;

namespace QuantConnect.DataProcessing
{
    public class Program
    {
        public static void Main()
        {
            // Use environment variable vs. config value, because the bash synchronization script
            // uses this environment variable to download historical data if requested.
            var processHistoricalData = Environment.GetEnvironmentVariable("PROCESS_HISTORICAL_DATA")?
                .ToLowerInvariant()
                .Trim() == "true";
            
            var deploymentDateValue = Environment.GetEnvironmentVariable("QC_DATAFLEET_DEPLOYMENT_DATE");
            var deploymentDate = Parse.DateTimeExact(deploymentDateValue, "yyyyMMdd", DateTimeStyles.None);

            var rawDataDirectory = new DirectoryInfo(Config.Get("raw-data-directory", Directory.GetCurrentDirectory()));
            var existingDataDirectory = new DirectoryInfo(Config.Get("processed-data-directory", Globals.DataFolder));
            var outputDataDirectory = Directory.CreateDirectory(Config.Get("temp-output-directory", "/temp-output-directory"));

            var converter = processHistoricalData 
                ? new ExtractAlphaTrueBeatsHistoricalConverter(deploymentDate, rawDataDirectory, existingDataDirectory, outputDataDirectory)
                : new ExtractAlphaTrueBeatsConverter(deploymentDate, rawDataDirectory, existingDataDirectory, outputDataDirectory);
            
            converter.Convert();
        }
    }
}