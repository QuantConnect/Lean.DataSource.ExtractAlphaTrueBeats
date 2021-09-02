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