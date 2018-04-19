
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
using System.IO;
using System.Net;

namespace CTlib
{
    ///
    /// CThttp_base
    /// 
    /// <summary>
    /// Abstract base class which extends CTwriter to support HTTP PUT.
    /// 
    /// The .NET Framework offers 3 methods of supporting HTTP:
    /// 1. HttpWebRequest: lower level API; the original method in .NET
    /// 2. WebClient:      simpler API, built on top of HttpWebRequest; used in CThttp
    /// 3. HttpClient:     newer API, requires .NET 4.5; used in CThttp_HttpClient; has the advantage that I/O calls are asynchronous; see https://docs.microsoft.com/en-us/dotnet/api/system.net.http.httpclient?view=netframework-4.7.1
    /// Articles comparing these methods:
    /// http://www.diogonunes.com/blog/webclient-vs-httpclient-vs-httpwebrequest/
    /// https://www.infoworld.com/article/3198673/web-development/my-two-cents-on-webclient-vs-httpclient-vs-httpwebrequest.html
    /// https://stackoverflow.com/questions/5140674/how-to-make-a-http-put-request
    /// 
    /// </summary>
    /// 
    public abstract class CThttp_base : CTwriter
    {
        /// <summary>
        /// The web host data will be PUT to; for example, "http://localhost:8000"
        /// </summary>
        protected String ctWebHost = "";

        /// <summary>
        /// Username/password credentials for logging into the server.
        /// </summary>
        protected NetworkCredential credential = null;

        /// <summary>
        /// Flag to indicate that username/password credentials have changed.
        /// </summary>
        protected bool bCredentialsChanged = false;

        /// <summary>
        /// The URL used for HTTP PUT.
        /// </summary>
        protected String urlStr = "";

        ///
        /// <summary>
        /// Constructor.
        /// 
        /// Calls the constructor in the base class.  Note that the following values are hard-wired when we call the base CTwriter constructor:
        /// numSegmentsToKeepI = 0, indicating to keep all segments
        /// bDeleteOldDataAtStartupI = false
        /// bVerifyOutputFolderI = false
        /// 
        /// </summary>
        /// <param name="baseCTOutputFolderI">The output source name along with optional additional sub-directory layers. Should be a relative folder path, since the absolute location where data will be written is determined by the HTTP server.</param>
        /// <param name="numBlocksPerSegmentI">Number of blocks per segment in the source folder hierarchy.  Use 0 to not include a segment layer.</param>
        /// <param name="bOutputTimesAreMillisI">Output times should be in milliseconds?  Needed if blocks are written (i.e., flush() is called) at a rate greater than 1Hz.</param>
        /// <param name="bPackI">Pack data at the block folder level?  Packed data times are linearly interpolated from the block start time to the time of the final datapoint in the packed channel.</param>
        /// <param name="bZipI">ZIP data at the block folder level?</param>
        /// <param name="ctWebHostI">The web host data will be PUT to.</param>
        ///
        public CThttp_base(String baseCTOutputFolderI, int numBlocksPerSegmentI, bool bOutputTimesAreMillisI, bool bPackI, bool bZipI, String ctWebHostI) : base(baseCTOutputFolderI, numBlocksPerSegmentI, 0, bOutputTimesAreMillisI, bPackI, bZipI, false, false)
        {
            ctWebHost = ctWebHostI;
            // Make sure ctWebHost does NOT end in '/'
            if (ctWebHost.EndsWith("/"))
            {
                ctWebHost = ctWebHost.Substring(0, ctWebHost.Length - 1);
            }
            Console.WriteLine("HTTP PUT data to {0}", ctWebHost);

            // Disable certificate validation (has the benefit of allowing acceptance of self-signed certificates)
            // Code to disable certificate validation was copied from Dominic Scheirlinck's response on Stack Overflow at
            //     https://stackoverflow.com/questions/526711/using-a-self-signed-certificate-with-nets-httpwebrequest-response
            // Sample author: Dominic Scheirlinck, https://stackoverflow.com/users/10831/dominic-scheirlinck
            // License: Stack Overflow content is covered by the Creative Commons license, https://creativecommons.org/licenses/by-sa/3.0/legalcode
            ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };
        }

        /// <summary>
        /// Create user credential for all subsequent HTTP PUT operations using the given username and password.
        /// </summary>
        /// <param name="userI">Username</param>
        /// <param name="pwI">Password</param>
        public void login(String userI, String pwI)
        {
            credential = new NetworkCredential(userI, pwI);
            bCredentialsChanged = true;
        }

        /// <summary>
        /// Low-level method to write data to the channel using HTTP PUT.
        /// </summary>
        /// <param name="outputDirI">Where the given data should be put on the server.</param>
        /// <param name="chanNameI">Channel name.</param>
        /// <param name="dataI">The data to PUT.</param>
        protected override void writeToStream(String outputDirI, String chanNameI, byte[] dataI)
        {
            urlStr = ctWebHost + "/" + outputDirI + chanNameI;

            // Replace Windows back-slash path separator with '/'
            if ( Path.DirectorySeparatorChar.ToString().Equals("\\") )
            {
                urlStr = urlStr.Replace(Path.DirectorySeparatorChar, '/');
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
