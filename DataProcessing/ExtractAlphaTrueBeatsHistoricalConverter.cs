/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using QuantConnect.Configuration;
using QuantConnect.DataSource;
using QuantConnect.Logging;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Converts ExtractAlpha historical TrueBeats data, starting from 2002-01-01 and ending on 2021-01-01
    /// </summary>
    public class ExtractAlphaTrueBeatsHistoricalConverter : ExtractAlphaTrueBeatsConverter
    {
        /// <summary>
        /// Creates a new instance of the TrueBeats historical data converter
        /// </summary>
        /// <param name="processingDate">The current processing date</param>
        /// <param name="rawDataDirectory"></param>
        /// <param name="existingDataDirectory"></param>
        /// <param name="outputDataDataDirectory"></param>
        public ExtractAlphaTrueBeatsHistoricalConverter(DirectoryInfo rawDataDirectory, DirectoryInfo existingDataDirectory, DirectoryInfo outputDataDataDirectory) 
            : base(DateTime.UtcNow.Date, rawDataDirectory, existingDataDirectory, outputDataDataDirectory)
        {
        }

        /// <summary>
        /// Converts TrueBeats historical data and outputs it in a format usable
        /// by the <see cref="ExtractAlphaTrueBeat"/> Reader(...) method.
        /// </summary>
        /// <exception cref="Exception">Historical data for fiscal periods was not found.</exception>
        public override void Convert()
        {
            var startDateValue = Config.Get("historical-processing-start-date", "2002-01-01");
            var endDateValue = Config.Get("historical-processing-end-date", "2021-02-01");

            var startDate = Parse.DateTimeExact(startDateValue, "yyyy-MM-dd", DateTimeStyles.None);
            var endDate = Parse.DateTimeExact(endDateValue, "yyyy-MM-dd", DateTimeStyles.None);
            
            // Split all of the historical data to disk to avoid running out of memory and allow for
            // code re-use of the base ExtractAlphaTrueBeatsConverter.
            ConvertHistoricalData(startDate);
            
            var currentProcessingDate = startDate;
            var processingData = ParseFiscalPeriods(currentProcessingDate);
            
            while (currentProcessingDate <= endDate)
            {
                currentProcessingDate += TimeSpan.FromDays(1);
                
                if (processingData.Count == 0)
                {
                    throw new Exception($"ExtractAlphaTrueBeatsHistoricalConverter.Convert(): No historical fiscal period data found. Exiting");
                }

                var processedData = ParseTrueBeats(processingData, currentProcessingDate);
                if (processedData.Count == 0)
                {
                    Log.Trace($"ExtractAlphaTrueBeatsHistoricalConverter.Convert(): No historical data exists for date: {currentProcessingDate:yyyy-MM-dd} - Skipping");
                    continue;
                }
                
                WriteToFile(processedData, currentProcessingDate);

                foreach (var trueBeats in processedData.Values)
                {
                    foreach (var trueBeat in trueBeats)
                    {
                        // Let's reset any values we previously set back to the default.
                        // We rely on ExpertBeat/TrendBeat/ManagementBeat to determine if there
                        // has been any duplicate data points for a single day.
                        // Unlikely, but in case a piece of data only appears in one dataset (All/FQ1) and not the other,
                        // resetting these values to the default would mimic the behavior of a newly
                        // initialized object, resulting in the correct outputted values.
                        trueBeat.AnalystEstimatesCount = default;
                        trueBeat.TrueBeat = default;
                        
                        trueBeat.ExpertBeat = null;
                        trueBeat.TrendBeat = null;
                        trueBeat.ManagementBeat = null;
                    }
                }
            }
        }

        /// <summary>
        /// Reads and re-organizes the historical data to be usable by the conversion methods defined in the base class.
        /// Data is outputted to the raw data directory, and is then parsed day by day.
        /// We split the data by date to avoid running out of memory, since some of these files are >10GB in size.
        /// </summary>
        private void ConvertHistoricalData(DateTime startDate)
        {
            Log.Trace($"ExtractAlphaTrueBeatsHistoricalConverter.ConvertHistoricalData(): Begin transforming raw historical data to format expected by parser");
            var earningsMetrics = new[]
            {
                ExtractAlphaTrueBeatEarningsMetric.EPS,
                ExtractAlphaTrueBeatEarningsMetric.Revenue
            };

            var rawAll = "All";
            var rawFQ1 = "FQ1";
            
            var rawDataTypes = new[]
            {
                rawAll,
                rawFQ1
            };

            var previousDate = startDate;
            var datesSkipped = new HashSet<string>();

            foreach (var rawDataType in rawDataTypes)
            {
                foreach (var earningsMetric in earningsMetrics)
                {
                    var metricName = earningsMetric == ExtractAlphaTrueBeatEarningsMetric.EPS
                        ? "EPS"
                        : "SALES";

                    var historicalDataFilePath = Path.Combine(
                        RawDataDirectory.FullName,
                        $"ExtractAlpha_{rawDataType}_TrueBeats_{metricName}_History_US_200001_20210131.csv");
                    
                    var outputLines = new List<string>();

                    // Since the historical data can be as large as 10GB for a single file,
                    // we'll need to load the data as we go along to make sure that we don't run
                    // out of memory.
                    using var fileStream = File.OpenRead(historicalDataFilePath);
                    using var reader = new StreamReader(fileStream);

                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (!char.IsNumber(line.FirstOrDefault()))
                        {
                            // Filter out headers and blank lines
                            continue;
                        }

                        var csv = line.Split(',');
                        
                        var dateValue = csv[0];
                        var date = Parse.DateTimeExact(dateValue, "yyyy-MM-dd", DateTimeStyles.None);
                        if (date < startDate) 
                        {
                            if (datesSkipped.Add(dateValue))
                            {
                                Log.Trace($"ExtractAlphaTrueBeatsHistoricalConverter.ConvertHistoricalData(): Skipping processing for date: {dateValue}");
                            }

                            continue;
                        }
                        
                        if (date != previousDate && outputLines.Count != 0)
                        {
                            Log.Trace($"ExtractAlphaTrueBeatsHistoricalConverter.ConvertHistoricalData(): Finished processing \"{rawDataType}\" {metricName} data for date: {previousDate:yyyy-MM-dd}");
                            WriteHistoricalDataToFile(previousDate, outputLines, metricName, rawDataType);
                            outputLines.Clear();
                        }

                        // The two datasets (All/FQ1) have the same columns for the first
                        // seven entries before we reach the point where there's data-specific
                        // data. Since we're only reformatting this data, we can include the next
                        // three columns that come after without worrying what type of data
                        // we're handling.
                        var lineData = new List<string>
                        {
                            dateValue,
                            csv[2],
                            csv[4],
                            csv[6],
                            csv[7],
                            csv[8],
                            csv[9]
                        };

                        if (rawDataType == rawFQ1)
                        {
                            // The "FQ1" dataset has three more columns with data than
                            // the "All" dataset, so we add them here.
                            lineData.Add(csv[10]);
                            lineData.Add(csv[11]);
                            lineData.Add(csv[12]);
                        }

                        outputLines.Add(string.Join(",", lineData));
                        previousDate = date;
                    }

                    if (outputLines.Count != 0)
                    {
                        // Final write, there might still be data we haven't written after the file finishes reading
                        Log.Trace($"ExtractAlphaTrueBeatsHistoricalConverter.ConvertHistoricalData(): Completed processing \"{rawDataType}\" {metricName} data");
                        WriteHistoricalDataToFile(previousDate, outputLines, metricName, rawDataType);
                    }
                }
            }
        }

        /// <summary>
        /// Gets and reformats the historical raw fiscal period data to the expected format
        /// </summary>
        /// <param name="processingDate">Processing date</param>
        /// <returns>Lines of CSV for the processing date</returns>
        protected override string[] GetFiscalPeriodRawLines(DateTime processingDate)
        {
            var rawLines = File.ReadAllLines(
                Path.Combine(
                    RawDataDirectory.FullName,
                    "ExtractAlpha_Fiscal_Periods_EPSSales_History_US_200001_20210131.csv"));

            // Skip blank/header lines and reformat the data
            return rawLines
                .Skip(1)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(line =>
                {
                    var csv = line.Split(',');
                    return string.Join(",",
                        $"{processingDate:yyyy-MM-dd}",
                        csv[1],
                        csv[3],
                        csv[5],
                        csv[6],
                        csv[7],
                        csv[8],
                        csv[9],
                        csv[10]);
                })
                .ToArray();
        }

        /// <summary>
        /// Loads TrueBeats historical data that we previously reformatted
        /// </summary>
        /// <param name="earningsMetric">Earnings type being forecasted</param>
        /// <param name="processingDate">Processing date</param>
        /// <param name="allQuarters">Load data for the "All" dataset</param>
        /// <returns>CSV lines</returns>
        protected override string[] GetTrueBeatsRawLines(
            ExtractAlphaTrueBeatEarningsMetric earningsMetric,
            DateTime processingDate,
            bool allQuarters)
        {
            try
            {
                return base.GetTrueBeatsRawLines(earningsMetric, processingDate, allQuarters);
            }
            catch
            {
                // Wrap in try/catch because the data might not exist on disk, and so
                // we would want to skip this day
                return Array.Empty<string>();
            }
        }

        /// <summary>
        /// Writes historical data to a local file
        /// </summary>
        /// <param name="date">Date to write data for</param>
        /// <param name="outputLines">CSV lines to write</param>
        /// <param name="metricName">The name of the metric being forecasted</param>
        /// <param name="rawDataType">The dataset type (All/FQ1)</param>
        private void WriteHistoricalDataToFile(
            DateTime date,
            List<string> outputLines,
            string metricName,
            string rawDataType)
        {
            File.WriteAllText(
                Path.Combine(
                    RawDataDirectory.FullName,
                    $"ExtractAlpha_{rawDataType}_TrueBeats_{metricName}_US_{date:yyyyMMdd}.csv"),
                string.Join("\n", outputLines));
        }
    }
}