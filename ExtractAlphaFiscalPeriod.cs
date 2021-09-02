using System;

namespace QuantConnect.DataSource
{
    public class ExtractAlphaFiscalPeriod
    {
        public int FiscalYear { get; set; }
            
        public int? FiscalQuarter { get; set; }

        public DateTime? End { get; set; }

        public DateTime? ExpectedReportDate { get; set; }
        
        public bool Annually => FiscalQuarter == null;
        
        public bool Quarterly => !Annually;
    }
}