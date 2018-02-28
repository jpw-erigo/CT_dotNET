
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

//
// JPW 2018-02-27
//
// This version of CThttp supports an asynchronous and a synchronous version
// of the WebClient.UploadData call.  The downside of the asynchronous method,
// as explained in the documentation below, is that subsequent requests for
// HTTP PUT while the asynchronous thread is busy are ignored, resulting in
// lost data.  A better way to handle asynchronous requests is to run the
// CTwriter.flush call in its own thread. 
//

using System;
using System.Threading;

namespace CTlib
{
    ///
    /// <summary>
    /// 
    /// CThttp
    /// 
    /// Child class of CThttp_base; uses WebClient to perform HTTP PUT.  Synchronous and asynchronous modes are supported.
    /// 
    /// Note that the asynchronous mode supported here has the downside that it will only do the next PUT if the previous
    /// PUT is finished; this can result in lost data.  A solution to this issue (as MJM has implemented in his Unity code)
    /// is to use a separate thread to handle the flush in CTlib.  In that case, if the previous flush isn't finished,
    /// we just wait until the next flush to try it again; there is no lost data becasue the data continues to be queued
    /// up waiting for the flush.
    /// 
    /// </summary>
    /// 
    public class CThttp : CThttp_base
    {

        private bool bAsync = false;

        // Member variables used by the asynchronous PUT method
        private Thread putThread = null;
        private byte[] _data = null;
        private Boolean doPutActive = false;
        private Boolean bExitDoPut = false;
        static EventWaitHandle _waitHandle = new AutoResetEvent(false);

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
            
            Console.WriteLine("==> using WebClient");
            
        }

        /// <summary>
        /// Specify whether HTTP PUT calls will be asynchronous.
        /// </summary>
        /// <param name="bAsyncI">Will HTTP PUT calls be asynchronous?</param>
        public void setAsyncHttpPut(bool bAsyncI)
        {
            if (bAsyncI != bAsync)
            {
                // User is requesting a change
                if (bAsync)
                {
                    // We are currently doing asynchronous PUTs; finish the current PUT before proceeding
                    waitForPutToFinish();
                }
                else
                {
                    // We are currently doing synchronous PUTs; change to asynchronous
                    putThread = new Thread(doPutContinuous);
                    putThread.Start();
                }
            }
            bAsync = bAsyncI;
            if (bAsync)
            {
                Console.WriteLine("Using asynchronous HTTP PUT");
            }
            else
            {
                Console.WriteLine("Using synchronous HTTP PUT");
            }
        }

        /// <summary>
        /// Call doPut continuously.  This method should be executed in a separate thread.  The execution loop in this method is under the control of an EventWaitHandle.
        /// </summary>
        private void doPutContinuous()
        {
            while (!bExitDoPut)
            {
                _waitHandle.WaitOne(); // Wait for notification
                if (_data != null)
                {
                    doPut(_data);
                }
                _data = null;
                doPutActive = false;
            }
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

            if (!bAsync)
            {
                // SYNCHRONOUS
                doPut(dataI);
            }
            else if (bAsync && !doPutActive)  // For asynchronous mode: only do the next PUT if the previous PUT is finished; this can result in lost data!
            {
                // ASYNCHRONOUS
                doPutActive = true;
                _data = dataI;        // Set data to be PUT
                _waitHandle.Set();    // Wake up the thread to do the put.
            }
            else if (bAsync)
            {
                Console.WriteLine("CThttp in async mode: prior PUT not finished, lost data");
            }

        }

        /// <summary>
        /// HTTP PUT the given data.
        /// </summary>
        /// <param name="dataI">The data to PUT.</param>
        private void doPut(byte[] dataI)
        {
            using (var client = new System.Net.WebClient())
            {
                // Console.WriteLine("HTTP PUT: {0}", urlStr);
                client.Credentials = credential;
                client.UploadData(urlStr, "PUT", dataI);
            }
        }

        /// <summary>
        /// Wait for pending HTTP PUT task to finish.
        /// </summary>
        private void waitForPutToFinish()
        {
            if (!bAsync)
            {
                return;
            }
            bExitDoPut = true;
            int maxNumSecToWait = 5;
            while (doPutActive && (maxNumSecToWait > 0))
            {
                System.Threading.Thread.Sleep(1000);
                --maxNumSecToWait;
            }
            if (doPutActive)
            {
                // There is still an active PUT going on; kill the thread
                putThread.Interrupt();
                putThread.Abort();
                _data = null;
            }
            else
            {
                // Thread isn't currently active; have the thread exit
                _data = null;
                _waitHandle.Set(); // Wake up the Waiter; this should cause doPutContinuous() to exit
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
            if (bAsync)
            {
                // Wait for the PUT to finish
                waitForPutToFinish();
            }
        }

    }
}
