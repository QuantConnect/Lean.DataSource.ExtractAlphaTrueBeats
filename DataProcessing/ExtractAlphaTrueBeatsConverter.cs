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
    /// Converts TrueBeats data into CSV parsable by the Reader(...) method of <see cref="ExtractAlphaTrueBeat"/>
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

            // The order we call these two functions in matters for now, but
            // won't in the future once I can filter duplicates more reliably.
            ParseTrueBeats(processingData, false);
            ParseTrueBeats(processingData, true);

            WriteToFile(processingData);
        }
        
        /// <summary>
        /// Parses raw TrueBeats data for EPS/Sales (revenue) 
        /// </summary>
        /// <param name="fiscalPeriods"></param>
        /// <param name="allQuarters"></param>
        protected void ParseTrueBeats(Dictionary<string, List<ExtractAlphaProcessingData>> fiscalPeriods, bool allQuarters)
        {
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
                    if (!fiscalPeriods.TryGetValue(ticker, out var symbolFiscalPeriods))
                    {
                        symbolFiscalPeriods = new List<ExtractAlphaProcessingData>();
                        fiscalPeriods[ticker] = symbolFiscalPeriods;
                    }

                    var (fiscalYear, fiscalQuarter) = ExtractAlphaProcessingData.ParseFiscalPeriod(csv[_fiscalPeriodIndex]);
                    if (!allQuarters && symbolFiscalPeriods.Any(x => 
                        x.EarningsMetric == earningsMetric &&
                        x.Data != null &&
                        x.FiscalPeriod.FiscalYear == fiscalYear &&
                        x.FiscalPeriod.FiscalQuarter == fiscalQuarter))
                    {
                        // We've encountered a duplicate Symbol while processing the FQ1 TrueBeats dataset, and
                        // the first piece of data has already been included.
                        // Majority of the duplicated symbols in the raw data will have duplicate data
                        // that equals another line in the same file. However, sometimes this isn't true
                        // and the TrueBeat calculation fluctuates between two numbers.
                        // TODO: send email to ExtractAlpha about this
                        if (_duplicateFQ1DataTickers.Add(ticker))
                        {
                            Log.Error($"ExtractAlphaTrueBeatsConverter.ParseTrueBeats(): Duplicate data encountered in FQ1 dataset for ticker: {ticker} - skipping");
                        }

                        continue;
                    }
                    
                    var processingData = symbolFiscalPeriods
                        .FirstOrDefault(x =>
                            x.EarningsMetric == earningsMetric && 
                            x.FiscalPeriod.FiscalYear == fiscalYear &&
                            x.FiscalPeriod.FiscalQuarter == fiscalQuarter);
                    
                    var insertProcessingData = processingData == null;

                    var analystCount = allQuarters
                        ? Parse.Int(csv[_analystCountIndex])
                        : (int?) null;
                    
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

                    processingData ??= new ExtractAlphaProcessingData
                    {
                        EarningsMetric = earningsMetric,
                        FiscalPeriod = new ExtractAlphaFiscalPeriod
                        {
                            FiscalYear = fiscalYear,
                            FiscalQuarter = fiscalQuarter
                        }
                    };

                    processingData.Data ??= new ExtractAlphaTrueBeat
                    {
                        EarningsMetric = earningsMetric,
                        FiscalPeriod = processingData.FiscalPeriod,
                        TrueBeat = trueBeat,
                        Time = _processingDate
                    };
                    
                    processingData.Data.AnalystCount = analystCount ?? processingData.Data.AnalystCount;
                    processingData.Data.ExpertBeat ??= expertBeat;
                    processingData.Data.TrendBeat ??= trendBeat;
                    processingData.Data.ManagementBeat ??= managementBeat;

                    if (insertProcessingData)
                    {
                        symbolFiscalPeriods.Add(processingData);
                    }
                }
            }
        }

        protected Dictionary<string, List<ExtractAlphaProcessingData>> ParseFiscalPeriods()
        {
            var fiscalPeriods = new Dictionary<string, List<ExtractAlphaProcessingData>>();
            
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
                
                var (fiscalYear, fiscalQuarter) = ExtractAlphaProcessingData.ParseFiscalPeriod(csv[6]);

                var fiscalPeriodEnd = Parse.DateTimeExact(csv[7], "yyyy-MM-dd", DateTimeStyles.None);
                var expectedReportDate = Parse.DateTimeExact(csv[8], "yyyy-MM-dd", DateTimeStyles.None);

                if (!fiscalPeriods.TryGetValue(ticker, out var symbolFiscalPeriods))
                {
                    symbolFiscalPeriods = new List<ExtractAlphaProcessingData>();
                    fiscalPeriods[ticker] = symbolFiscalPeriods;
                }
                else if (symbolFiscalPeriods.Any(x =>
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
                
                symbolFiscalPeriods.Add(new ExtractAlphaProcessingData
                {
                    EarningsMetric = earningsMetric,
                    FiscalPeriod = new ExtractAlphaFiscalPeriod 
                    {
                        FiscalYear = fiscalYear,
                        FiscalQuarter = fiscalQuarter,
                        End = fiscalPeriodEnd,
                        ExpectedReportDate = expectedReportDate
                    }
                });
            }

            return fiscalPeriods;
        }

        private void WriteToFile(Dictionary<string, List<ExtractAlphaProcessingData>> processingData)
        {
            foreach (var kvp in processingData)
            {
                var ticker = kvp.Key;
                var trueBeatData = kvp.Value
                    .Select(x => x.Data)
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
                    .Select(ExtractAlphaProcessingData.ToCsv)
                    .ToList();
                   
                var existingDataLinesSet = outputDataLines.ToHashSet();

                foreach (var trueBeat in trueBeatData)
                {
                    var csvLine = ExtractAlphaProcessingData.ToCsv(trueBeat);
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

        protected virtual string[] GetFiscalPeriodRawLines()
        {
            return File.ReadAllLines(
                Path.Combine(
                    _rawDataDirectory.FullName, 
                    $"Fiscal_Periods_EPSSales_US_{_processingDate:yyyyMMdd}.csv"));
        }

        protected virtual string[] GetTrueBeatsRawLines(
            ExtractAlphaTrueBeatEarningsMetric earningsMetric,
            bool allQuarters)
        {
            var earningsMetricValue = earningsMetric == ExtractAlphaTrueBeatEarningsMetric.EPS
                ? "EPS"
                : "SALES";

            return File.ReadAllLines($"ExtractAlpha_{(allQuarters ? "All" : "FQ1")}_TrueBeats_{earningsMetricValue}_US_{_processingDate:yyyyMMdd}.csv");
        }

        public class ExtractAlphaProcessingData
        {
            public ExtractAlphaTrueBeatEarningsMetric EarningsMetric { get; set; }
            
            public ExtractAlphaFiscalPeriod FiscalPeriod { get; set; }
            
            public ExtractAlphaTrueBeat Data { get; set; }

            public static Tuple<int, int?> ParseFiscalPeriod(string fiscalPeriodEntry)
            {
                var fiscalPeriodSplit = fiscalPeriodEntry.Split(' ');
                var fiscalYear = Parse.Int(fiscalPeriodSplit[0]);
                var fiscalQuarter = fiscalPeriodSplit.Length > 1
                    ? Parse.Int(fiscalPeriodSplit[1].Replace("Q", ""))
                    : (int?)null;

                return new Tuple<int, int?>(fiscalYear, fiscalQuarter);
            }

            public static string ToCsv(ExtractAlphaTrueBeat trueBeat)
            {
                return string.Join(",",
                    trueBeat.Time.ToStringInvariant("yyyyMMdd"),
                    trueBeat.EarningsMetric.ToString().ToLowerInvariant(),
                    $"{trueBeat.AnalystCount}",
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
}
