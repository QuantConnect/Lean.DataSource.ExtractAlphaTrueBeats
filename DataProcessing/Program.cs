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
                ? new ExtractAlphaTrueBeatsHistoricalConverter(rawDataDirectory, existingDataDirectory, outputDataDirectory)
                : new ExtractAlphaTrueBeatsConverter(deploymentDate, rawDataDirectory, existingDataDirectory, outputDataDirectory);
            
            converter.Convert();
        }
    }
}