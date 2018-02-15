
/*
Copyright 2017 Erigo Technologies LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using System;
using System.IO;

namespace CTlib
{
    ///
    /// <summary>
    /// CThttp
    /// 
    /// Child class of CTwriter which supports writing data using HTTP PUT.
    /// </summary>
    /// 
    public class CThttp : CTwriter
    {
        private String ctWebHost = "";

        ///
        /// <summary>
        /// Constructor.
        /// 
        /// Calls the constructor in the base class.  Note that the following values are hard-wired when we call the base constructor:
        /// numSegmentsToKeepI = 0, indicating to keep all segments
        /// bDeleteOldDataAtStartupI = false
        /// bVerifyOutputFolderI = false
        /// 
        /// </summary>
        /// <param name="baseCTOutputFolderI">The root folder where the output source is to be written. This must be at the same level as the application working directory or a sub-directory under it.</param>
        /// <param name="numBlocksPerSegmentI">Number of blocks per segment in the source folder hierarchy.  Use 0 to not include a segment layer.</param>
        /// <param name="bOutputTimesAreMillisI">Output times should be in milliseconds?  Needed if blocks are written (i.e., flush() is called) at a rate greater than 1Hz.</param>
        /// <param name="bPackI">Pack data at the block folder level?  Packed data times are linearly interpolated from the block start time to the time of the final datapoint in the packed channel.</param>
        /// <param name="bZipI">ZIP data at the block folder level?</param>
        /// <param name="ctWebHostI">Optional argument; this is the web host data will be PUT to.</param>
        ///
        public CThttp(String baseCTOutputFolderI, int numBlocksPerSegmentI, bool bOutputTimesAreMillisI, bool bPackI, bool bZipI, String ctWebHostI = "http://localhost:8000") : base(baseCTOutputFolderI, numBlocksPerSegmentI, 0, bOutputTimesAreMillisI, bPackI, bZipI, false, false)
        {
            ctWebHost = ctWebHostI;
            // Make sure ctWebHost does NOT end in '/'
            if (ctWebHost.EndsWith("/"))
            {
                ctWebHost = ctWebHost.Substring(0, ctWebHost.Length - 1);
            }
            Console.WriteLine("HTTP PUT data to {0}", ctWebHost);
        }

        /// <summary>
        /// Low-level method to write data to the channel using HTTP PUT.
        /// 
        /// See https://stackoverflow.com/questions/5140674/how-to-make-a-http-put-request
        /// for some ideas how to use HTTP PUT from C#.
        /// </summary>
        /// <param name="outputDirI"></param>
        /// <param name="chanNameI"></param>
        /// <param name="dataI"></param>
        protected override void writeToStream(String outputDirI, String chanNameI, byte[] dataI)
        {
            String urlStr = ctWebHost + "/" + outputDirI + chanNameI;

            // Replace Windows back-slash path separator with '/'
            if ( Path.DirectorySeparatorChar.ToString().Equals("\\") )
            {
                urlStr = urlStr.Replace(Path.DirectorySeparatorChar, '/');
            }

            using (var client = new System.Net.WebClient())
            {
                // Console.WriteLine("HTTP PUT: {0}", urlStr);
                client.UploadData(urlStr, "PUT", dataI);
            }
        }

    }
}
