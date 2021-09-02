
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using NodaTime;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    public class ExtractAlphaTrueBeat : BaseData
    {
        public ExtractAlphaFiscalPeriod FiscalPeriod { get; set; }

        public ExtractAlphaTrueBeatEarningsMetric EarningsMetric { get; set; }

        public int AnalystCount { get; set; }

        public decimal TrueBeat { get; set; }
        
        public decimal? ExpertBeat { get; set; }
        
        public decimal? TrendBeat { get; set; }
        
        public decimal? ManagementBeat { get; set; }

        public override DateTime EndTime { get; set; }
        

        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "extractalpha",
                    "truebeats",
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"),
                SubscriptionTransportMedium.LocalFile,
                FileFormat.Csv);
        }

        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');

            var time = Parse.DateTimeExact(csv[0], "yyyyMMdd", DateTimeStyles.None);
            var earningsMetric = (ExtractAlphaTrueBeatEarningsMetric)Enum.Parse(typeof(ExtractAlphaTrueBeatEarningsMetric), csv[1], true);
            var analystCount = Parse.Int(csv[2]);
            var trueBeat = Parse.Decimal(csv[3], NumberStyles.Any);

            var expertBeat = !string.IsNullOrEmpty(csv[4])
                ? Parse.Decimal(csv[4], NumberStyles.Any)
                : (decimal?) null;

            var trendBeat = !string.IsNullOrEmpty(csv[5])
                ? Parse.Decimal(csv[5], NumberStyles.Any)
                : (decimal?) null;

            var managementBeat = !string.IsNullOrEmpty(csv[6])
                ? Parse.Decimal(csv[6], NumberStyles.Any)
                : (decimal?) null;

            var fiscalYear = Parse.Int(csv[7]);

            var fiscalQuarter = !string.IsNullOrEmpty(csv[8])
                ? Parse.Int(csv[8])
                : (int?) null;

            var fiscalPeriodEnd = !string.IsNullOrEmpty(csv[9])
                ? Parse.DateTimeExact(csv[9], "yyyyMMdd", DateTimeStyles.None)
                : (DateTime?) null;

            var expectedReportDate = !string.IsNullOrEmpty(csv[10])
                ? Parse.DateTimeExact(csv[10], "yyyyMMdd", DateTimeStyles.None)
                : (DateTime?) null;

            return new ExtractAlphaTrueBeat
            {
                EarningsMetric = earningsMetric,
                FiscalPeriod = new ExtractAlphaFiscalPeriod
                {
                    FiscalYear = fiscalYear,
                    FiscalQuarter = fiscalQuarter,
                    End = fiscalPeriodEnd,
                    ExpectedReportDate = expectedReportDate
                },

                AnalystCount = analystCount,
                TrueBeat = trueBeat,

                ExpertBeat = expertBeat,
                TrendBeat = trendBeat,
                ManagementBeat = managementBeat,
                
                Time = date,
                EndTime = date.Date.AddHours(12).AddMinutes(30),
                Symbol = config.Symbol
            };
        }

        public override BaseData Clone()
        {
            return new ExtractAlphaTrueBeat
            {
                EarningsMetric = EarningsMetric,
                FiscalPeriod = FiscalPeriod,

                AnalystCount = AnalystCount,
                TrueBeat = TrueBeat,

                ExpertBeat = ExpertBeat,
                TrendBeat = TrendBeat,
                ManagementBeat = ManagementBeat,
                
                Time = Time,
                EndTime = EndTime,
                Symbol = Symbol
            };
        }

        public override bool RequiresMapping()
        {
            return true;
        }

        public override bool IsSparseData()
        {
            return true;
        }

        public override List<Resolution> SupportedResolutions()
        {
            return AllResolutions;
        }

        public override DateTimeZone DataTimeZone()
        {
            return TimeZones.NewYork;
        }

        public override string ToString()
        {
            return "";
        }
    }
}