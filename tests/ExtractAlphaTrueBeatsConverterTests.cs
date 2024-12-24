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
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.DataProcessing;
using QuantConnect.DataSource;
using QuantConnect.Interfaces;
using QuantConnect.Util;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class ExtractAlphaTrueBeatsConverterTests
    {
        [OneTimeSetUp]
        public void Setup()
        {
            Composer.Instance.GetExportedValueByTypeName<IMapFileProvider>(Configuration.Config.Get("map-file-provider", typeof(LocalDiskMapFileProvider).Name));
        }

        private static List<string> _fiscalPeriodLines = new List<string>
        {
            "Date,Ticker,CUSIP,ISIN,Item,Period_Type,Fiscal_Period,Period_End_Date,Report_Date",
            "2021-05-01,AAPL,0123456789,US0000000000,EPS,A,2021,2021-12-31,2022-01-31",
            "2021-05-01,AAPL,0123456789,US0000000000,EPS,Q,2021 Q2,2021-06-30,2021-08-15",
            "2021-05-01,AAPL,0123456789,US0000000000,SALES,A,2021,2021-12-31,2022-01-31",
            "2021-05-01,AAPL,0123456789,US0000000000,SALES,Q,2021 Q2,2021-06-30,2021-08-15",
            "2021-05-01,GOOG,1123456789,US0000000001,EPS,A,2021,2021-12-31,2022-01-31",
            "2021-05-01,GOOG,1123456789,US0000000001,EPS,Q,2021 Q2,2021-06-30,2021-08-15",
            "2021-05-01,GOOG,1123456789,US0000000001,SALES,A,2021,2021-12-31,2022-01-31",
            "2021-05-01,GOOG,1123456789,US0000000001,SALES,Q,2021 Q2,2021-06-30,2021-08-15",
            "2021-05-01,FB,1223456789,US0000000002,EPS,A,2021,2021-12-31,2022-01-31",
            "2021-05-01,FB,1223456789,US0000000002,EPS,Q,2021 Q2,2021-06-30,2021-08-15",
            "2021-05-01,FB,1223456789,US0000000002,SALES,A,2021,2021-12-31,2022-01-31",
            "2021-05-01,FB,1223456789,US0000000002,SALES,Q,2021 Q2,2021-06-30,2021-08-15",
            // Duplicate should be excluded from final result
            "2021-05-01,FB,1223456789,US0000000002,SALES,Q,2021 Q2,2021-06-30,2021-08-15"
        };
        private static List<string> _trueBeatsAllRawLines = new List<string>()
        {
            "2021-05-01,AAPL,0123456789,US0000000000,2021,10,0.543210",
            "2021-05-01,AAPL,0123456789,US0000000000,2021 Q2,10,0.543210",
            "2021-05-01,GOOG,1123456789,US0000000001,2021,20,0.123456",
            "2021-05-01,GOOG,1123456789,US0000000001,2021 Q2,20,0.123456",
            // This should match up with fiscal period and FQ1 data
            "2021-05-01,FB,1223456789,US0000000002,2021 Q2,50,0.8888",
            // This is a duplicate data point and should be excluded from the final result
            "2021-05-01,FB,1223456789,US0000000002,2021 Q2,50,0.11111",
            // This result should not match up with any fiscal period data in this test
            "2021-05-01,FB,1223456789,US0000000002,2021 Q3,90,0.9999"
        };
        private static List<string> _trueBeatsFQ1RawLines = new List<string>
        {
            "2021-05-01,AAPL,0123456789,US0000000000,2021 Q2,2021-08-15,0.543210,0.34,0.3032,-0.09999",
            "2021-05-01,GOOG,1123456789,US0000000001,2021 Q2,2021-08-15,0.123456,0.123,0.000456,0",
            "2021-05-01,FB,1223456789,US0000000002,2021 Q2,2021-08-15,0.8888,0.2222,0.2222,0.4444",
            // Duplicated line should be excluded from final result
            "2021-05-01,FB,1223456789,US0000000002,2021 Q2,2021-08-15,0.9999,0.3333,0.3333,0.3333",
        };
        
        private static ExtractAlphaFiscalPeriod _annualFiscal = new ExtractAlphaFiscalPeriod
        {
            FiscalYear = 2021,
            FiscalQuarter = null,
            End = new DateTime(2021, 12, 31),
            ExpectedReportDate = new DateTime(2022, 1, 31)
        };
        private static ExtractAlphaFiscalPeriod _quarterlyFiscal = new ExtractAlphaFiscalPeriod
        {
            FiscalYear = 2021,
            FiscalQuarter = 2,
            End = new DateTime(2021, 6, 30),
            ExpectedReportDate = new DateTime(2021, 8, 15)
        };
        private static ExtractAlphaFiscalPeriod _missingFiscal = new ExtractAlphaFiscalPeriod
        {
            FiscalYear = 2021,
            FiscalQuarter = 3,
            End = null,
            ExpectedReportDate = null
        };

        private static DateTime _date = new DateTime(2021, 5, 1);
        private static DateTime _endTime = _date.AddHours(12).AddMinutes(30);
        
        private static Symbol _aapl = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);
        private static Symbol _goog= Symbol.Create("GOOG", SecurityType.Equity, Market.USA);
        private static Symbol _fb= Symbol.Create("FB", SecurityType.Equity, Market.USA);
        
        private static Dictionary<string, List<ExtractAlphaTrueBeat>> _expectedData = new Dictionary<string, List<ExtractAlphaTrueBeat>>
        {
            {"AAPL", new List<ExtractAlphaTrueBeat>
            {
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _annualFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS,
                    AnalystEstimatesCount = 10,
                    TrueBeat = 0.54321m,
                    ExpertBeat = null,
                    TrendBeat = null,
                    ManagementBeat = null,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _aapl
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _quarterlyFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS,
                    AnalystEstimatesCount = 10,
                    TrueBeat = 0.54321m,
                    ExpertBeat = 0.34m,
                    TrendBeat = 0.3032m,
                    ManagementBeat = -0.09999m,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _aapl
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _annualFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue,
                    AnalystEstimatesCount = 10,
                    TrueBeat = 0.54321m,
                    ExpertBeat = null,
                    TrendBeat = null,
                    ManagementBeat = null,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _aapl
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _quarterlyFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue,
                    AnalystEstimatesCount = 10,
                    TrueBeat = 0.54321m,
                    ExpertBeat = 0.34m,
                    TrendBeat = 0.3032m,
                    ManagementBeat = -0.09999m,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _aapl
                },
            }},
            {"GOOG", new List<ExtractAlphaTrueBeat>
            {
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _annualFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS,
                    AnalystEstimatesCount = 20,
                    TrueBeat = 0.123456m,
                    ExpertBeat = null,
                    TrendBeat = null,
                    ManagementBeat = null,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _goog
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _quarterlyFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS,
                    AnalystEstimatesCount = 20,
                    TrueBeat = 0.123456m,
                    ExpertBeat = 0.123m,
                    TrendBeat = 0.000456m,
                    ManagementBeat = 0,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _goog
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _annualFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue,
                    AnalystEstimatesCount = 20,
                    TrueBeat = 0.123456m,
                    ExpertBeat = null,
                    TrendBeat = null,
                    ManagementBeat = null,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _goog
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _quarterlyFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue,
                    AnalystEstimatesCount = 20,
                    TrueBeat = 0.123456m,
                    ExpertBeat = 0.123m,
                    TrendBeat = 0.000456m,
                    ManagementBeat = 0,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _goog
                },
            }},
            {"FB", new List<ExtractAlphaTrueBeat>
            {
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _quarterlyFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS,
                    AnalystEstimatesCount = 50,
                    TrueBeat = 0.8888m,
                    ExpertBeat = 0.2222m,
                    TrendBeat = 0.2222m,
                    ManagementBeat = 0.4444m,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _fb
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _missingFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS,
                    AnalystEstimatesCount = 90,
                    TrueBeat = 0.9999m,
                    ExpertBeat = null,
                    TrendBeat = null,
                    ManagementBeat = null,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _fb
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _quarterlyFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue,
                    AnalystEstimatesCount = 50,
                    TrueBeat = 0.8888m,
                    ExpertBeat = 0.2222m,
                    TrendBeat = 0.2222m,
                    ManagementBeat = 0.4444m,
                        
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _fb
                },
                new ExtractAlphaTrueBeat
                {
                    FiscalPeriod = _missingFiscal,
                    EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue,
                    AnalystEstimatesCount = 90,
                    TrueBeat = 0.9999m,
                    ExpertBeat = null,
                    TrendBeat = null,
                    ManagementBeat = null,
                    
                    Time = _date,
                    EndTime = _endTime,
                    Symbol = _fb
                }
            }}
        };

        [Test]
        public void ConvertsData()
        {
            var converter = new TestingExtractAlphaTrueBeatsConverter(
                _date,
                _fiscalPeriodLines,
                _trueBeatsAllRawLines,
                _trueBeatsFQ1RawLines);
            
            converter.Convert();

            var actual = converter.Result;

            Assert.IsNotNull(actual);
            Assert.IsNotEmpty(actual);

            Assert.IsTrue(actual.ContainsKey("AAPL"));
            Assert.IsTrue(actual.ContainsKey("GOOG"));
            Assert.IsTrue(actual.ContainsKey("FB"));
            
            foreach (var kvp in actual)
            {
                var ticker = kvp.Key;
                var trueBeats = kvp.Value;
                var expectedData = _expectedData[ticker];

                Assert.AreEqual(expectedData.Count, trueBeats.Count);

                for (var i = 0; i < trueBeats.Count; i++)
                {
                    AssertTrueBeat(expectedData[i], trueBeats[i]);
                }
            }
        }

        [Test]
        public void ParsesRawFiscalData()
        {
            var converter = new TestingExtractAlphaTrueBeatsConverter(
                _date,
                _fiscalPeriodLines,
                null,
                null);

            var tickerFiscalPeriods = converter.TestParseFiscalPeriods();
            Assert.AreEqual(3, tickerFiscalPeriods.Keys.Count);

            var expectedFiscalPeriodData = _expectedData
                .Where(kvp => kvp.Key != "FB")
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            expectedFiscalPeriodData["FB"] = new List<ExtractAlphaTrueBeat>
            {
                new ExtractAlphaTrueBeat { FiscalPeriod = _annualFiscal, EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS },
                new ExtractAlphaTrueBeat { FiscalPeriod = _quarterlyFiscal, EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.EPS },
                new ExtractAlphaTrueBeat { FiscalPeriod = _annualFiscal, EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue },
                new ExtractAlphaTrueBeat { FiscalPeriod = _quarterlyFiscal, EarningsMetric = ExtractAlphaTrueBeatEarningsMetric.Revenue }
            };
            
            foreach (var kvp in tickerFiscalPeriods)
            {
                var ticker = kvp.Key;
                
                var expectedFiscalPeriods= expectedFiscalPeriodData[ticker];
                var actualFiscalPeriods = kvp.Value;
                Assert.AreEqual(expectedFiscalPeriods.Count, actualFiscalPeriods.Count);
                
                for (var i = 0; i < actualFiscalPeriods.Count; i++)
                {
                    var expectedFiscalPeriod = expectedFiscalPeriods[i].FiscalPeriod;
                    var actualFiscalPeriod = actualFiscalPeriods[i].FiscalPeriod;
                    
                    Assert.AreEqual(expectedFiscalPeriods[i].EarningsMetric, actualFiscalPeriods[i].EarningsMetric);
                    
                    Assert.AreEqual(expectedFiscalPeriod.FiscalYear, actualFiscalPeriod.FiscalYear);
                    Assert.AreEqual(expectedFiscalPeriod.FiscalQuarter, actualFiscalPeriod.FiscalQuarter);
                    Assert.AreEqual(expectedFiscalPeriod.End, actualFiscalPeriod.End);
                    Assert.AreEqual(expectedFiscalPeriod.ExpectedReportDate, actualFiscalPeriod.ExpectedReportDate);
                }
            }
        }

        [TestCase("2019 Q1", 2019, 1, false)]
        [TestCase("2020 Q1", 2020, 1, false)]
        [TestCase("2024 Q1", 2024, 1, false)]
        [TestCase("2019 Q2", 2019, 2, false)]
        [TestCase("2024 Q2", 2024, 2, false)]
        [TestCase("2019 Q3", 2019, 3, false)]
        [TestCase("2022 Q3", 2022, 3, false)]
        [TestCase("2024 Q3", 2024, 3, false)]
        [TestCase("2019 Q4", 2019, 4, false)]
        [TestCase("2020 Q4", 2020, 4, false)]
        [TestCase("2024 Q4", 2024, 4, false)]
        [TestCase("2021 Q", 2021, null, true)]
        [TestCase("2021 2", 2021, 2, false)]
        [TestCase("2020.05 Q2", 2020, 2, true)]
        public void ParsesFiscalPeriods(string fiscalPeriod, int expectedFiscalYear, int? expectedFiscalQuarter, bool failureExpected)
        {
            try
            {
                var fiscalPeriodParsed = TestingExtractAlphaTrueBeatsConverter.TestParseFiscalPeriod(fiscalPeriod);
                if (failureExpected)
                {
                    Assert.Fail("Expected failure, but parsed data successfully");
                }
                
                Assert.AreEqual(expectedFiscalYear, fiscalPeriodParsed.Item1);
                Assert.AreEqual(expectedFiscalQuarter, fiscalPeriodParsed.Item2);
            }
            catch
            {
                if (failureExpected)
                {
                    return;
                }

                // Re-throw if we weren't expecting failure so that the test runner
                // gets more information about the exception thrown.
                throw;
            }
        }

        [TestCase("AAPL", 0, "20210501,eps,10,0.54321,,,,2021,,20211231,20220131")]
        [TestCase("AAPL", 1, "20210501,eps,10,0.54321,0.34,0.3032,-0.09999,2021,2,20210630,20210815")]
        [TestCase("AAPL", 2, "20210501,revenue,10,0.54321,,,,2021,,20211231,20220131")]
        [TestCase("AAPL", 3, "20210501,revenue,10,0.54321,0.34,0.3032,-0.09999,2021,2,20210630,20210815")]
        [TestCase("GOOG", 0, "20210501,eps,20,0.123456,,,,2021,,20211231,20220131")]
        [TestCase("GOOG", 1, "20210501,eps,20,0.123456,0.123,0.000456,0,2021,2,20210630,20210815")]
        [TestCase("GOOG", 2, "20210501,revenue,20,0.123456,,,,2021,,20211231,20220131")]
        [TestCase("GOOG", 3, "20210501,revenue,20,0.123456,0.123,0.000456,0,2021,2,20210630,20210815")] 
        [TestCase("FB", 0, "20210501,eps,50,0.8888,0.2222,0.2222,0.4444,2021,2,20210630,20210815")]
        [TestCase("FB", 1, "20210501,eps,90,0.9999,,,,2021,3,,")]
        [TestCase("FB", 2, "20210501,revenue,50,0.8888,0.2222,0.2222,0.4444,2021,2,20210630,20210815")]
        [TestCase("FB", 3, "20210501,revenue,90,0.9999,,,,2021,3,,")]
        public void ToCsvTest(string ticker, int index, string expectedLine)
        {
            var actualLine = TestingExtractAlphaTrueBeatsConverter.TestToCsv(_expectedData[ticker][index]);
            Assert.AreEqual(expectedLine, actualLine);
        }
        
        [TestCase("AAPL", 0, "20210501,eps,10,0.54321,,,,2021,,20211231,20220131")]
        [TestCase("AAPL", 1, "20210501,eps,10,0.54321,0.34,0.3032,-0.09999,2021,2,20210630,20210815")]
        [TestCase("AAPL", 2, "20210501,revenue,10,0.54321,,,,2021,,20211231,20220131")]
        [TestCase("AAPL", 3, "20210501,revenue,10,0.54321,0.34,0.3032,-0.09999,2021,2,20210630,20210815")]
        [TestCase("GOOG", 0, "20210501,eps,20,0.123456,,,,2021,,20211231,20220131")]
        [TestCase("GOOG", 1, "20210501,eps,20,0.123456,0.123,0.000456,0,2021,2,20210630,20210815")]
        [TestCase("GOOG", 2, "20210501,revenue,20,0.123456,,,,2021,,20211231,20220131")]
        [TestCase("GOOG", 3, "20210501,revenue,20,0.123456,0.123,0.000456,0,2021,2,20210630,20210815")] 
        [TestCase("FB", 0, "20210501,eps,50,0.8888,0.2222,0.2222,0.4444,2021,2,20210630,20210815")]
        [TestCase("FB", 1, "20210501,eps,90,0.9999,,,,2021,3,,")]
        [TestCase("FB", 2, "20210501,revenue,50,0.8888,0.2222,0.2222,0.4444,2021,2,20210630,20210815")]
        [TestCase("FB", 3, "20210501,revenue,90,0.9999,,,,2021,3,,")]
        [TestCase("AAPL", 0, "20210531,eps,10,0.54321,,,,2021,,20211231,20220131", false)]
        public void ReaderFromCsvTests(string ticker, int index, string line, bool datesEqual = true)
        {
            var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);
            var config = new SubscriptionDataConfig(
                typeof(ExtractAlphaTrueBeat),
                symbol,
                Resolution.Tick,
                TimeZones.NewYork,
                TimeZones.NewYork,
                false,
                false,
                false,
                true);

            var factory = new ExtractAlphaTrueBeat();
            var expected = _expectedData[ticker][index];
            var actual = (ExtractAlphaTrueBeat)factory.Reader(config, line, _date, false);
            
            AssertTrueBeat(expected, actual);
            Assert.AreEqual(expected.Symbol, actual.Symbol);

            if (!datesEqual)
            {
                // Checks if we're reading the date from the data
                // and not using the date passed in to the function.
                Assert.AreNotEqual(expected.Time, actual.Time);
                Assert.AreNotEqual(expected.EndTime, actual.EndTime);
                return;
            }
            
            Assert.AreEqual(expected.Time, actual.Time);
            Assert.AreEqual(expected.EndTime, actual.EndTime);
        }

        private void AssertTrueBeat(ExtractAlphaTrueBeat expected, ExtractAlphaTrueBeat actual)
        {
            Assert.AreEqual(expected.FiscalPeriod.FiscalYear, actual.FiscalPeriod.FiscalYear);
            Assert.AreEqual(expected.FiscalPeriod.FiscalQuarter, actual.FiscalPeriod.FiscalQuarter);
            Assert.AreEqual(expected.FiscalPeriod.End, actual.FiscalPeriod.End);
            Assert.AreEqual(expected.FiscalPeriod.ExpectedReportDate, actual.FiscalPeriod.ExpectedReportDate);
                    
            Assert.AreEqual(expected.EarningsMetric, actual.EarningsMetric);
            Assert.AreEqual(expected.AnalystEstimatesCount, actual.AnalystEstimatesCount);
            Assert.AreEqual(expected.TrueBeat, actual.TrueBeat);
            Assert.AreEqual(expected.ExpertBeat, actual.ExpertBeat);
            Assert.AreEqual(expected.TrendBeat, actual.TrendBeat);
            Assert.AreEqual(expected.ManagementBeat, actual.ManagementBeat);
        }
        
        private class TestingExtractAlphaTrueBeatsConverter : ExtractAlphaTrueBeatsConverter
        {
            private readonly DateTime _date;
            private List<string> _fiscalPeriodRawLines;
            private List<string> _trueBeatsRawLines;
            private List<string> _trueBeatsRawLinesFQ1;

            public Dictionary<string, List<ExtractAlphaTrueBeat>> Result { get; set; }
            
            public TestingExtractAlphaTrueBeatsConverter(
                DateTime processingDate,
                List<string> fiscalPeriodRawLines,
                List<string> trueBeatsRawLines,
                List<string> trueBeatsRawLinesFQ1)
                : base(processingDate, null, null, null)
            {
                _date = processingDate;
                _fiscalPeriodRawLines = fiscalPeriodRawLines ?? new List<string>();
                _trueBeatsRawLines = trueBeatsRawLines ?? new List<string>();
                _trueBeatsRawLinesFQ1 = trueBeatsRawLinesFQ1 ?? new List<string>();
            }

            public override void Convert()
            {
                var processingData = ParseFiscalPeriods(_date);
                Result = ParseTrueBeats(processingData, _date);
            }

            public void TestParseTrueBeats(Dictionary<string, List<ExtractAlphaTrueBeat>> trueBeats)
            {
                ParseTrueBeats(trueBeats, _date);
            }

            public Dictionary<string, List<ExtractAlphaTrueBeat>> TestParseFiscalPeriods()
            {
                return ParseFiscalPeriods(_date);
            }

            protected override string[] GetFiscalPeriodRawLines(DateTime processingDate)
            {
                return _fiscalPeriodRawLines.ToArray();
            }

            protected override string[] GetTrueBeatsRawLines(ExtractAlphaTrueBeatEarningsMetric earningsMetric, DateTime processingDate, bool allQuarters)
            {
                return allQuarters
                    ? _trueBeatsRawLines.ToArray()
                    : _trueBeatsRawLinesFQ1.ToArray();
            }

            public static Tuple<int, int?> TestParseFiscalPeriod(string fiscalPeriodEntry)
            {
                return ParseFiscalPeriod(fiscalPeriodEntry);
            }

            public static string TestToCsv(ExtractAlphaTrueBeat trueBeat)
            {
                return ToCsv(trueBeat);
            }
        }
    }
}