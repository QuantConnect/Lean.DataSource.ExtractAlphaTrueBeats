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

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Fiscal period that the <see cref="ExtractAlphaTrueBeat"/> instance has forecasts for.
    /// </summary>
    public class ExtractAlphaFiscalPeriod
    {
        /// <summary>
        /// Fiscal year (i.e. the year that the financial report applies to in 10-Q and/or 10-K SEC filings)
        /// </summary>
        public int FiscalYear { get; set; }
         
        /// <summary>
        /// Fiscal quarter (i.e. the quarter that the financial report applies to in 10-Q filings).
        /// If this is null, then the fiscal period being reported is for the full year of the <see cref="FiscalYear"/>
        /// </summary>
        public int? FiscalQuarter { get; set; }

        /// <summary>
        /// The date that the fiscal quarter ends
        /// </summary>
        public DateTime? End { get; set; }

        /// <summary>
        /// The date that the SEC report for the fiscal period is expected to be released publicly
        /// </summary>
        public DateTime? ExpectedReportDate { get; set; }
        
        /// <summary>
        /// Returns true if the fiscal period is for the whole fiscal year (all quarters)
        /// </summary>
        public bool Annual => FiscalQuarter == null;
        
        /// <summary>
        /// Returns true if the fiscal period is for a single quarter only
        /// </summary>
        public bool Quarterly => !Annual;
    }
}