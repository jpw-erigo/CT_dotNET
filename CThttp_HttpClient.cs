
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
using System.Collections;
using System.Net.Http;

namespace CTlib
{
    ///
    /// CThttp_HttpClient
    /// 
    /// <summary>
    /// Child class of CThttp_base; uses HttpClient to perform asynchronous HTTP PUT.
    /// 
    /// This class has been tested and works, but is still somewhat "experimental";
    /// for example, the method used in closeHttpClient() to wait for pending
    /// requests before closing the connection is kludgey (there's probably a
    /// better way to do this).
    /// </summary>
    /// 
    public class CThttp_HttpClient : CThttp_base
    {
        private static HttpClient httpClient = null;
        private static HttpClientHandler hch = null;

        // For keeping track of async HTTP PUT calls
        private static long putIdx = 0;
        private static ArrayList requestList = new ArrayList();

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
        public CThttp_HttpClient(String baseCTOutputFolderI, int numBlocksPerSegmentI, bool bOutputTimesAreMillisI, bool bPackI, bool bZipI, String ctWebHostI = "http://localhost:8000") : base(baseCTOutputFolderI, numBlocksPerSegmentI, bOutputTimesAreMillisI, bPackI, bZipI, ctWebHostI)
        {
            
            Console.WriteLine("HTTP PUTs using HttpClient");
            
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

            if ((httpClient == null) || bCredentialsChanged)
            {
                if (httpClient != null)
                {
                    // Close the existing HttpClient connection
                    closeHttpClient();
                }
                hch = new HttpClientHandler();
                if (credential != null)
                {
                    hch.Credentials = credential;
                }
                httpClient = new HttpClient(hch);
                // Set 30-sec timeout
                httpClient.Timeout = new TimeSpan(0, 0, 30);
                bCredentialsChanged = false;
            }

            // The PutAsync method is "fire-and-forget": this async method will return
            // before it has finished, but that is as we intend it; we just let
            // PutAsync finish in the background.  If we wanted to call PutAsync and
            // then wait here for it to finish, prefix this call with "await".
            PutAsync(urlStr, dataI);
            
        }

        /// <summary>
        /// Asynchronous method to put data using HttpClient.
        /// 
        /// This is intended to be a "fire-and-forget" method; that is, the caller calls
        /// this method and then it lumbers on while this PutAsyn method does its stuff.
        /// If we want the caller to keep track of this method or wait for it to be complete,
        /// then this method could return System.Threading.Tasks.Task and the caller could
        /// "await" on it.
        /// 
        /// Here's a good Microsoft article giving an overview of asynchronous programming
        /// using the "async" and "await" keywords:
        /// https://msdn.microsoft.com/en-us/library/hh191443(v=vs.120).aspx
        /// </summary>
        /// <param name="urlStrI">URL where to PUT the data.</param>
        /// <param name="dataI">The data to PUT.</param>
        /// <returns></returns>
        private static async void PutAsync(String urlStrI, byte[] dataI)
        {
            long localIdx = ++putIdx;
            requestList.Add(localIdx);
            try
            {
                var result = await httpClient.PutAsync(urlStrI, new ByteArrayContent(dataI));
                // Console.WriteLine(result.StatusCode);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            requestList.Remove(localIdx);
        }

        /// <summary>
        /// Wait for pending HTTP PUT tasks to finish and then close the HttpClient.
        /// </summary>
        private void closeHttpClient()
        {
            if (httpClient != null)
            {
                if (requestList.Count > 0)
                {
                    int pendingTaskCount = requestList.Count;
                    for (int i=0; i<30; ++i)
                    {
                        pendingTaskCount = requestList.Count;
                        Console.WriteLine("Waiting on " + pendingTaskCount + " async PUT tasks");
                        System.Threading.Thread.Sleep(2000);
                        if (requestList.Count == 0)
                        {
                            break;
                        }
                        if (requestList.Count == pendingTaskCount)
                        {
                            // The number isn't going down, just quit
                            Console.WriteLine("Closing connections; may lose " + pendingTaskCount + " PUTs");
                            break;
                        }
                    }
                }
                httpClient.CancelPendingRequests();
                httpClient.Dispose();
            }
            requestList.Clear();
            httpClient = null;
        }

        ///
        /// <summary>
        /// Close the source.
        /// </summary>
        /// 
        public override void close()
        {
            Console.WriteLine("CThttp_HttpClient closing");
            base.close();
            closeHttpClient();
        }

    }
}
