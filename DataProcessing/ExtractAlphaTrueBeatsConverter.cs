using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using QuantConnect.Data;
using QuantConnect.DataSource;
using QuantConnect.Logging;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Converts TrueBeats data into CSV lines parsable by the Reader(...) method of <see cref="ExtractAlphaTrueBeat"/>
    /// </summary>
    public class ExtractAlphaTrueBeatsConverter
    {
        private const int _tickerIndex = 1;
        private const int _fiscalPeriodIndex = 4;
        private const int _trueBeatIndex = 6;
        private const int _analystCountIndex = 5;
        private const int _expertBeatIndex = 7;
        private const int _trendBeatIndex = 8;
        private const int _managementBeatIndex = 9; 
        
        private readonly DateTime _processingDate;
        private readonly DirectoryInfo _rawDataDirectory;
        private readonly DirectoryInfo _existingDataDirectory;
        private readonly DirectoryInfo _outputDataDirectory;
        private readonly HashSet<string> _duplicateFiscalDataTickers = new HashSet<string>();
        private readonly HashSet<string> _duplicateFQ1DataTickers = new HashSet<string>();
        
        private static readonly ExtractAlphaTrueBeat _factory = new ExtractAlphaTrueBeat();
        private static readonly ExtractAlphaTrueBeatEarningsMetric[] _earningsMetrics = {
            ExtractAlphaTrueBeatEarningsMetric.EPS,
            ExtractAlphaTrueBeatEarningsMetric.Revenue
        };
        
        private static readonly SubscriptionDataConfig _config = new SubscriptionDataConfig(
            typeof(object),
            Symbol.None,
            default,
            TimeZones.Utc,
            TimeZones.Utc,
            false,
            false,
            false);

        /// <summary>
        /// Creates an instance of the converter for the provided processing date
        /// </summary>
        /// <param name="processingDate">Date to process data for</param>
        /// <param name="rawDataDirectory">Directory that raw data is directly under</param>
        /// <param name="existingDataDirectory">Base directory of the data folder where existing data is</param>
        /// <param name="outputDataDataDirectory"></param>
        public ExtractAlphaTrueBeatsConverter(
            DateTime processingDate, 
            DirectoryInfo rawDataDirectory,
            DirectoryInfo existingDataDirectory,
            DirectoryInfo outputDataDataDirectory)
        {
            _processingDate = processingDate;
            _rawDataDirectory = rawDataDirectory;
            _existingDataDirectory = existingDataDirectory;
            _outputDataDirectory = outputDataDataDirectory;
        }
        
        /// <summary>
        /// Converts the raw data into CSV
        /// </summary>
        public void Convert()
        {
            var processingData = ParseFiscalPeriods();

            ParseTrueBeats(processingData);
            WriteToFile(processingData);
        }
        
        /// <summary>
        /// Parses raw TrueBeats data for EPS/Revenue
        /// </summary>
        /// <param name="trueBeats">The TrueBeats data parsed so far containing the fiscal periods of the data</param>
        protected void ParseTrueBeats(Dictionary<string, List<ExtractAlphaTrueBeat>> trueBeats)
        {
            // Process the TrueBeats "All" dataset and then joins it with the "FQ1" dataset.
            var allQuarters = false;

            do
            {
                // Once we begin processing "FQ1", this variable will be equal
                // to false and end the loop.
                allQuarters = !allQuarters;

                foreach (var earningsMetric in _earningsMetrics)
                {
                    foreach (var line in GetTrueBeatsRawLines(earningsMetric, allQuarters))
                    {
                        if (!char.IsNumber(line.FirstOrDefault()))
                        {
                            // Skips the CSV header line and empty lines
                            continue;
                        }

                        var csv = line.Split(',');

                        var ticker = csv[_tickerIndex];
                        if (!trueBeats.TryGetValue(ticker, out var symbolTrueBeats))
                        {
                            // Not all tickers appear in the fiscal periods dataset, so we'll need to initialize
                            // a new list for these tickers that won't have fiscal year information.
                            symbolTrueBeats = new List<ExtractAlphaTrueBeat>();
                            trueBeats[ticker] = symbolTrueBeats;
                        }

                        var (fiscalYear, fiscalQuarter) = ParseFiscalPeriod(csv[_fiscalPeriodIndex]);
                        if (!allQuarters && symbolTrueBeats.Any(x =>
                            x.EarningsMetric == earningsMetric &&
                            x.FiscalPeriod.FiscalYear == fiscalYear &&
                            x.FiscalPeriod.FiscalQuarter == fiscalQuarter &&
                            x.ExpertBeat != null &&
                            x.TrendBeat != null &&
                            x.ManagementBeat != null))
                        {
                            // We've encountered a duplicate Symbol while processing the FQ1 TrueBeats dataset, and
                            // the first piece of data has already been included.
                            // Majority of the duplicated symbols in the raw data will have duplicate data
                            // that equals another line in the same file. However, sometimes this isn't true
                            // and the TrueBeat calculation fluctuates between two numbers.
                            if (_duplicateFQ1DataTickers.Add(ticker))
                            {
                                Log.Error($"ExtractAlphaTrueBeatsConverter.ParseTrueBeats(): Duplicate data encountered in FQ1 dataset for ticker: {ticker} - skipping");
                            }

                            continue;
                        }

                        var processingData = symbolTrueBeats
                            .FirstOrDefault(x =>
                                x.EarningsMetric == earningsMetric &&
                                x.FiscalPeriod.FiscalYear == fiscalYear &&
                                x.FiscalPeriod.FiscalQuarter == fiscalQuarter);

                        // If we didn't find a match, this is the first time that we've encountered
                        // data for the ticker with a fiscal year/quarter we haven't seen before.
                        var insertProcessingData = processingData == null;

                        var analystCount = allQuarters
                            ? Parse.Int(csv[_analystCountIndex])
                            : (int?) null;

                        // TrueBeat will always be present and not null in both "FQ1" and "All" datasets.
                        var trueBeat = Parse.Decimal(csv[_trueBeatIndex], NumberStyles.Any);
                        var expertBeat = !allQuarters
                            ? Parse.Decimal(csv[_expertBeatIndex], NumberStyles.Any)
                            : (decimal?) null;

                        var trendBeat = !allQuarters
                            ? Parse.Decimal(csv[_trendBeatIndex], NumberStyles.Any)
                            : (decimal?) null;

                        var managementBeat = !allQuarters
                            ? Parse.Decimal(csv[_managementBeatIndex], NumberStyles.Any)
                            : (decimal?) null;

                        // Only initializes the `processingData` variable if it's currently null.
                        processingData ??= new ExtractAlphaTrueBeat
                        {
                            EarningsMetric = earningsMetric,
                            FiscalPeriod = new ExtractAlphaFiscalPeriod
                            {
                                FiscalYear = fiscalYear,
                                FiscalQuarter = fiscalQuarter
                            },
                            Time = _processingDate
                        };

                        processingData.AnalystEstimatesCount = analystCount ?? processingData.AnalystEstimatesCount;
                        processingData.TrueBeat = trueBeat;
                        processingData.ExpertBeat ??= expertBeat;
                        processingData.TrendBeat ??= trendBeat;
                        processingData.ManagementBeat ??= managementBeat;

                        if (expertBeat != null && trendBeat != null && managementBeat != null)
                        {
                            // If we're processing a duplicate, then we should calculate the TrueBeat
                            // value ourselves, in case the data is ordered differently between the
                            // FQ1 and All raw data. TrueBeat is the sum of the ExpertBeat, TrendBeat, and ManagementBeat
                            processingData.TrueBeat = expertBeat.Value + trendBeat.Value + managementBeat.Value;
                        }

                        if (insertProcessingData)
                        {
                            // It's the first time we've seen data for this fiscal year/quarter, so
                            // let's include it so that it can be written to disk later.
                            symbolTrueBeats.Add(processingData);
                        }
                    }
                }
            } while (allQuarters);
        }

        /// <summary>
        /// Parses the Fiscal Periods dataset to get more information about the
        /// fiscal year and quarter for all stocks.
        /// </summary>
        /// <returns>
        /// Dictionary keyed by ticker that can or will contain data that is written to disk.
        /// </returns>
        protected Dictionary<string, List<ExtractAlphaTrueBeat>> ParseFiscalPeriods()
        {
            var trueBeats = new Dictionary<string, List<ExtractAlphaTrueBeat>>();
            
            foreach (var line in GetFiscalPeriodRawLines())
            {
                if (!char.IsNumber(line.FirstOrDefault()))
                {
                    // Skips the CSV header line and empty lines
                    continue;
                }
                
                var csv = line.Split(',');

                var ticker = csv[1];
                var earningsMetricValue = csv[4].ToLowerInvariant();
                if (earningsMetricValue != "eps" && earningsMetricValue != "sales")
                {
                    Log.Error($"Encountered unknown earnings metric: {csv[4]} - skipping");
                    continue;
                }

                var earningsMetric = earningsMetricValue == "eps"
                    ? ExtractAlphaTrueBeatEarningsMetric.EPS
                    : ExtractAlphaTrueBeatEarningsMetric.Revenue;
                
                var (fiscalYear, fiscalQuarter) = ParseFiscalPeriod(csv[6]);

                var fiscalPeriodEnd = Parse.DateTimeExact(csv[7], "yyyy-MM-dd", DateTimeStyles.None);
                var expectedReportDate = Parse.DateTimeExact(csv[8], "yyyy-MM-dd", DateTimeStyles.None);

                if (!trueBeats.TryGetValue(ticker, out var symbolTrueBeats))
                {
                    symbolTrueBeats = new List<ExtractAlphaTrueBeat>();
                    trueBeats[ticker] = symbolTrueBeats;
                }
                else if (symbolTrueBeats.Any(x =>
                    x.EarningsMetric == earningsMetric &&
                    x.FiscalPeriod.FiscalYear == fiscalYear &&
                    x.FiscalPeriod.FiscalQuarter == fiscalQuarter &&
                    x.FiscalPeriod.End == fiscalPeriodEnd &&
                    x.FiscalPeriod.ExpectedReportDate == expectedReportDate))
                {
                    // We've encountered a duplicate Symbol while processing the Fiscal_Periods_EPSSales TrueBeats dataset, and
                    // the first piece of data has already been included.
                    // Majority of the duplicated symbols in the raw data will have duplicate data
                    // that equals another line in the same file. 
                    // TODO: send email to ExtractAlpha about this
                    if (_duplicateFiscalDataTickers.Add(ticker))
                    {
                        Log.Error($"ExtractAlphaTrueBeatsConverter.ParseTrueBeats(): Duplicate data encountered in Fiscal_Periods_EPSSales dataset for ticker: {ticker} - skipping");
                    }

                    continue;
                }
                
                symbolTrueBeats.Add(new ExtractAlphaTrueBeat
                {
                    EarningsMetric = earningsMetric,
                    FiscalPeriod = new ExtractAlphaFiscalPeriod 
                    {
                        FiscalYear = fiscalYear,
                        FiscalQuarter = fiscalQuarter,
                        End = fiscalPeriodEnd,
                        ExpectedReportDate = expectedReportDate
                    },
                    Time = _processingDate
                });
            }

            return trueBeats;
        }

        /// <summary>
        /// Writes processed data to disk, merging any existing data with the newly processed data.
        /// </summary>
        /// <param name="processingData">The data to write to disk</param>
        private void WriteToFile(Dictionary<string, List<ExtractAlphaTrueBeat>> processingData)
        {
            foreach (var kvp in processingData)
            {
                var ticker = kvp.Key;
                var trueBeatData = kvp.Value
                    .OrderBy(x => x.FiscalPeriod.FiscalYear)
                    .ThenBy(x => x.FiscalPeriod.FiscalQuarter ?? 0)
                    .ThenBy(x => x.EarningsMetric);

                var outputData = new List<ExtractAlphaTrueBeat>();
                var processedData = new FileInfo(
                    Path.Combine(
                        _existingDataDirectory.FullName,
                        "alternative",
                        "extractalpha",
                        "truebeats",
                        $"{ticker.ToLowerInvariant()}.csv"));

                if (processedData.Exists)
                {
                    outputData = File.ReadAllLines(processedData.FullName)
                        .Select(x => (ExtractAlphaTrueBeat) _factory.Reader(_config, x, _processingDate, false))
                        .OrderBy(x => x.Time)
                        .ThenBy(x => x.FiscalPeriod.FiscalYear)
                        .ThenBy(x => x.FiscalPeriod.FiscalQuarter ?? 0)
                        .ThenBy(x => x.EarningsMetric)
                        .ToList();
                }

                var outputDataLines = outputData
                    .Select(ToCsv)
                    .ToList();
                   
                var existingDataLinesSet = outputDataLines.ToHashSet();

                foreach (var trueBeat in trueBeatData)
                {
                    var csvLine = ToCsv(trueBeat);
                    if (existingDataLinesSet.Add(csvLine))
                    {
                        outputDataLines.Add(csvLine);
                    }
                }

                var outputFileDirectory = Directory.CreateDirectory(
                    Path.Combine(
                        _outputDataDirectory.FullName,
                        "alternative",
                        "extractalpha",
                        "truebeats"));
                
                var outputFilePath = Path.Combine(
                    outputFileDirectory.FullName, 
                    $"{ticker.ToLowerInvariant()}.csv");
                
                Log.Trace($"ExtractAlphaTrueBeatsConverter.WriteToFile(): Writing data to: {outputFilePath}");
                File.WriteAllText(outputFilePath, string.Join("\n", outputDataLines));
            }
        }

        /// <summary>
        /// Gets the Fiscal Period dataset's contents
        /// </summary>
        /// <returns>Array of all lines inside the dataset</returns>
        /// <remarks>Made virtual and protected for inserting test cases in unit tests</remarks>
        protected virtual string[] GetFiscalPeriodRawLines()
        {
            return File.ReadAllLines(
                Path.Combine(
                    _rawDataDirectory.FullName, 
                    $"Fiscal_Periods_EPSSales_US_{_processingDate:yyyyMMdd}.csv"));
        }

        /// <summary>
        /// Gets the TrueBeat dataset's contents
        /// </summary>
        /// <param name="earningsMetric">The earning metric to load data for</param>
        /// <param name="allQuarters">If true, load data for the "All" dataset</param>
        /// <returns>Array of all lines inside the dataset</returns>
        /// <remarks>Made virtual and protected for inserting test cases in unit tests</remarks>
        protected virtual string[] GetTrueBeatsRawLines(
            ExtractAlphaTrueBeatEarningsMetric earningsMetric,
            bool allQuarters)
        {
            var earningsMetricValue = earningsMetric == ExtractAlphaTrueBeatEarningsMetric.EPS
                ? "EPS"
                : "SALES";

            return File.ReadAllLines($"ExtractAlpha_{(allQuarters ? "All" : "FQ1")}_TrueBeats_{earningsMetricValue}_US_{_processingDate:yyyyMMdd}.csv");
        }

        /// <summary>
        /// Parses the fiscal period as it appears in the raw data
        /// </summary>
        /// <param name="fiscalPeriodEntry">Fiscal period data (e.g. "2021 Q3")</param>
        /// <returns>Tuple of (Fiscal Year, Fiscal Quarter (nullable))</returns>
        protected static Tuple<int, int?> ParseFiscalPeriod(string fiscalPeriodEntry)
        {
            // Separate the fiscal year and the quarter from each other
            var fiscalPeriodSplit = fiscalPeriodEntry.Split(' ');
            
            // Fiscal year will always be there, no need for a null check
            var fiscalYear = Parse.Int(fiscalPeriodSplit[0]);
            
            // But fiscal quarter can be null, so check to see it exists, and if so,
            // parse the number next to the `Q` to get the fiscal quarter number.
            var fiscalQuarter = fiscalPeriodSplit.Length > 1
                ? Parse.Int(fiscalPeriodSplit[1].Replace("Q", ""))
                : (int?)null;

            return new Tuple<int, int?>(fiscalYear, fiscalQuarter);
        }

        /// <summary>
        /// Converts a <see cref="ExtractAlphaTrueBeat"/> to CSV, readable by the Reader(...) method of the TrueBeat class.
        /// </summary>
        /// <param name="trueBeat">TrueBeat data</param>
        /// <returns>Line of CSV for the TrueBeat data</returns>
        protected static string ToCsv(ExtractAlphaTrueBeat trueBeat)
        {
            return string.Join(",",
                trueBeat.Time.ToStringInvariant("yyyyMMdd"),
                trueBeat.EarningsMetric.ToString().ToLowerInvariant(),
                $"{trueBeat.AnalystEstimatesCount}",
                $"{trueBeat.TrueBeat.NormalizeToStr()}",
                $"{trueBeat.ExpertBeat?.NormalizeToStr()}",
                $"{trueBeat.TrendBeat?.NormalizeToStr()}",
                $"{trueBeat.ManagementBeat?.NormalizeToStr()}",
                $"{trueBeat.FiscalPeriod.FiscalYear}",
                $"{trueBeat.FiscalPeriod.FiscalQuarter}",
                trueBeat.FiscalPeriod.End?.ToStringInvariant("yyyyMMdd"),
                trueBeat.FiscalPeriod.ExpectedReportDate?.ToStringInvariant("yyyyMMdd"));
        }
    }
}