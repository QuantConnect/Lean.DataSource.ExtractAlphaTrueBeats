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
using System.IO;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.DataProcessing;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class ExtractAlphaTrueBeatsConverterTests
    {
        [Test]
        public void ConvertsData()
        {
            var time = new DateTime(2021, 5, 1);
            var fiscalPeriodLines = new List<string>
            {
                "Date,Ticker,CUSIP,ISIN,Item,Period_Type,Fiscal_Period,Period_End_Date,Report_Date",
                "2021-05-01,AAPL,0123456789,US0000000000,EPS,A,2021,2021-12-31,2022-01-31",
                "2021-05-01,AAPL,0123456789,US0000000000,EPS,Q,2021 Q2,2021-06-30,2021-08-15",
                "2021-05-01,AAPL,0123456789,US0000000000,SALES,Q,2021 Q2,2021-06-30,2021-08-15",
                "2021-05-01,GOOG,1123456789,US0000000001,EPS,A,2021,2021-12-31,2022-01-31",
                "2021-05-01,GOOG,1123456789,US0000000001,EPS,Q,2021 Q2,2021-06-30,2021-08-15",
                "2021-05-01,GOOG,1123456789,US0000000001,SALES,Q,2021 Q2,2021-06-30,2021-08-15"
            };
            var trueBeatsRawLinesEPS = new List<string>
            {
            };
            var converter = new TestingExtractAlphaTrueBeatsConverter(
                time,
                null,
                null,
                null)
            {
                FiscalPeriodRawLines = fiscalPeriodLines,
            };
        }

        private class TestingExtractAlphaTrueBeatsConverter : ExtractAlphaTrueBeatsConverter
        {
            public List<string> FiscalPeriodRawLines { get; set; } = new List<string>();
            
            public List<string> TrueBeatsRawLinesEPS { get; set; } = new List<string>();
            
            public List<string> TrueBeatsRawLinesEPS_FQ1 { get; set; } = new List<string>();
            
            public List<string> TrueBeatsRawLinesRevenue { get; set; } = new List<string>();
            
            public List<string> TrueBeatsRawLinesRevenue_FQ1 { get; set; } = new List<string>();
            
            public TestingExtractAlphaTrueBeatsConverter(
                DateTime processingDate,
                DirectoryInfo rawDataDirectory,
                DirectoryInfo existingDataDirectory,
                DirectoryInfo outputDataDataDirectory) 
                : base(processingDate, rawDataDirectory, existingDataDirectory, outputDataDataDirectory)
            {
            }

            public void TestParseTrueBeats(Dictionary<string, List<ExtractAlphaTrueBeat>> trueBeats)
            {
                ParseTrueBeats(trueBeats);
            }

            public Dictionary<string, List<ExtractAlphaTrueBeat>> TestParseFiscalPeriods()
            {
                return ParseFiscalPeriods();
            }

            protected override string[] GetFiscalPeriodRawLines()
            {
                return FiscalPeriodRawLines.ToArray();
            }

            protected override string[] GetTrueBeatsRawLines(ExtractAlphaTrueBeatEarningsMetric earningsMetric, bool allQuarters)
            {
                return (earningsMetric, allQuarters) switch
                {
                    (ExtractAlphaTrueBeatEarningsMetric.EPS, true) => TrueBeatsRawLinesEPS.ToArray(),
                    (ExtractAlphaTrueBeatEarningsMetric.EPS, false) => TrueBeatsRawLinesEPS_FQ1.ToArray(),
                    (ExtractAlphaTrueBeatEarningsMetric.Revenue, true) => TrueBeatsRawLinesRevenue.ToArray(),
                    (ExtractAlphaTrueBeatEarningsMetric.Revenue, false) => TrueBeatsRawLinesRevenue_FQ1.ToArray(),
                    _ => throw new Exception($"The provided {earningsMetric.ToString()} is not supported for this test")
                };
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