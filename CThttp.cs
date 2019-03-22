
/*
Copyright 2018 Erigo Technologies LLC

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

namespace CTlib
{
    ///
    /// CThttp
    /// 
    /// <summary>
    /// Child class of CThttp_base; uses WebClient to perform HTTP PUT.
    /// </summary>
    /// 
    public class CThttp : CThttp_base
    {

        ///
        /// <summary>
        /// Constructor.
        /// 
        /// Calls the constructor in the base class.
        /// 
        /// </summary>
        /// <param name="baseCTOutputFolderI">The output source name along with optional additional sub-directory layers. Should be a relative folder path, since the absolute location where data will be written is determined by the HTTP server.</param>
        /// <param name="numBlocksPerSegmentI">Number of blocks per segment in the source folder hierarchy.  Use 0 to not include a segment layer.</param>
        /// <param name="bOutputTimesAreMillisI">Output times should be in milliseconds?  Needed if blocks are written (i.e., flush() is called) at a rate greater than 1Hz.</param>
        /// <param name="bPackI">Pack data at the block folder level?  Packed data times are linearly interpolated from the block start time to the time of the final datapoint in the packed channel.</param>
        /// <param name="bZipI">ZIP data at the block folder level?</param>
        /// <param name="ctWebHostI">Optional argument; this is the web host data will be PUT to.</param>
        ///
        public CThttp(String baseCTOutputFolderI, int numBlocksPerSegmentI, bool bOutputTimesAreMillisI, bool bPackI, bool bZipI, String ctWebHostI = "http://localhost:8000") : base(baseCTOutputFolderI, numBlocksPerSegmentI, bOutputTimesAreMillisI, bPackI, bZipI, ctWebHostI)
        {
            
            Console.WriteLine("HTTP PUTs using WebClient");
            
        }

        /// <summary>
        /// Write data to the channel using HTTP PUT.
        /// </summary>
        /// <param name="outputDirI">Where the given data should be put on the server.</param>
        /// <param name="chanNameI">Channel name.</param>
        /// <param name="dataI">The data to PUT.</param>
        protected override void writeToStream(String outputDirI, String chanNameI, byte[] dataI)
        {
            base.writeToStream(outputDirI, chanNameI, dataI);

            // Code to issue HTTP PUT using WebClient from response on Stack Overflow at
            //     https://stackoverflow.com/questions/5140674/how-to-make-a-http-put-request
            // Sample authors:
            //     Danny Beckett, https://stackoverflow.com/users/1563422/danny-beckett
            //     Marc Gravell, https://stackoverflow.com/users/23354/marc-gravell
            // License: Stack Overflow content is covered by the Creative Commons license, https://creativecommons.org/licenses/by-sa/3.0/legalcode
            using (var client = new System.Net.WebClient())
            {
                // Console.WriteLine("HTTP PUT: {0}", urlStr);
                client.Credentials = credential;
                try
                {
                    client.UploadData(urlStr, "PUT", dataI);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception caught from UploadData (HTTP PUT):\n{0}", e.InnerException);
                }
            }
        }

        ///
        /// <summary>
        /// Close the source.
        /// </summary>
        /// 
        public override void close()
        {
            base.close();
        }

    }
}
