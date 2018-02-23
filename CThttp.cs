
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
using System.IO;
using System.Net;
using System.Net.Http;

namespace CTlib
{
    ///
    /// <summary>
    /// 
    /// CThttp
    /// 
    /// Child class of CTwriter which supports writing data using HTTP PUT.
    /// 
    /// The .NET Framework offers 3 methods of supporting HTTP:
    /// 1. HttpWebRequest: lower level API; the original method in .NET
    /// 2. WebClient:      simpler API, built on top of HttpWebRequest
    /// 3. HttpClient:     newer API, requires .NET 4.5; has the advantage that I/O calls are asynchronous; see https://docs.microsoft.com/en-us/dotnet/api/system.net.http.httpclient?view=netframework-4.7.1
    /// Articles comparing these methods:
    /// http://www.diogonunes.com/blog/webclient-vs-httpclient-vs-httpwebrequest/
    /// https://www.infoworld.com/article/3198673/web-development/my-two-cents-on-webclient-vs-httpclient-vs-httpwebrequest.html
    /// 
    /// In this class, we support HTTP PUT using WebClient and there is an "experimental" support
    /// for HttpClient (enabled by setting bAsync to true).
    /// 
    /// </summary>
    /// 
    public class CThttp : CTwriter
    {
        private String ctWebHost = "";
        private NetworkCredential credential = null;
        private static HttpClient httpClient = null;
        private static HttpClientHandler hch = null;
        private bool bAsync = false;
        private bool bCredentialsChanged = false;

        // For keeping track of async HTTP PUT calls
        private static long putIdx = 0;
        private static ArrayList requestList = new ArrayList();

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

            // Options for dealing with SSL certificates
            // See https://stackoverflow.com/questions/526711/using-a-self-signed-certificate-with-nets-httpwebrequest-response for details.
            // OPTION 1: register a method for custom validation of SSL server certificates
            // ServicePointManager.ServerCertificateValidationCallback = ValidateServerCertficate;
            // OPTION 2: Disable certificate validation (has the benefit of allowing acceptance of self-signed certificates)
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
        /// Set whether we will do asynchronous HTTP PUT.
        /// </summary>
        /// <param name="userI">Username</param>
        /// <param name="pwI">Password</param>
        public void setAsyncHttpPut(bool bAsyncI)
        {
            if (bAsyncI != bAsync)
            {
                // There's going to be a change;
                // either way it goes, reset httpClient.
                if (httpClient != null)
                {
                    closeHttpClient();
                }
            }
            bAsync = bAsyncI;
            if (bAsync)
            {
                Console.WriteLine("Using async HTTP PUT (based on HttpClient)");
            }
            else
            {
                Console.WriteLine("Using synchronous HTTP PUT (based on WebClient)");
            }
        }

        /// <summary>
        /// Low-level method to write data to the channel using HTTP PUT.
        /// 
        /// See https://stackoverflow.com/questions/5140674/how-to-make-a-http-put-request
        /// for some ideas how to use HTTP PUT from C#.
        /// </summary>
        /// <param name="outputDirI">Where the given data should be put on the server.</param>
        /// <param name="chanNameI">Channel name.</param>
        /// <param name="dataI">The data to PUT.</param>
        protected override void writeToStream(String outputDirI, String chanNameI, byte[] dataI)
        {
            String urlStr = ctWebHost + "/" + outputDirI + chanNameI;

            // Replace Windows back-slash path separator with '/'
            if ( Path.DirectorySeparatorChar.ToString().Equals("\\") )
            {
                urlStr = urlStr.Replace(Path.DirectorySeparatorChar, '/');
            }

            if (bAsync)
            {
                if ( (httpClient == null) || bCredentialsChanged )
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
                PutAsync(urlStr, dataI);
            }
            else
            {
                using (var client = new System.Net.WebClient())
                {
                    // Console.WriteLine("HTTP PUT: {0}", urlStr);
                    client.Credentials = credential;
                    client.UploadData(urlStr, "PUT", dataI);
                }
            }
            
        }

        /// <summary>
        /// Asynchronous method to put data using HttpClient
        /// 
        /// Here's a good Microsoft article giving an overview of asynchronous programming
        /// using the "async" and "await" keywords:
        /// https://msdn.microsoft.com/en-us/library/hh191443(v=vs.120).aspx
        /// </summary>
        /// <param name="urlStrI">URL where to PUT the data.</param>
        /// <param name="dataI">The data to PUT.</param>
        /// <returns></returns>
        private static async System.Threading.Tasks.Task PutAsync(String urlStrI, byte[] dataI)
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
                    for (int i = 0; i < 20; ++i)
                    {
                        pendingTaskCount = requestList.Count;
                        Console.WriteLine("Waiting on " + pendingTaskCount + " async PUT tasks");
                        System.Threading.Thread.Sleep(1000);
                        if (requestList.Count == 0)
                        {
                            break;
                        }
                        if (requestList.Count == pendingTaskCount)
                        {
                            // The number isn't going down, wait one more second and then quit
                            System.Threading.Thread.Sleep(1000);
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
            base.close();
            closeHttpClient();
        }

        /// <summary>
        /// Prompt user whether they want to install unknown certificates.
        /// Code from https://stackoverflow.com/questions/526711/using-a-self-signed-certificate-with-nets-httpwebrequest-response
        /// </summary>
        /// <param name="sender">An object that contains state information for this validation.</param>
        /// <param name="cert">The certificate used to authenticate the remote party.</param>
        /// <param name="chain">The chain of certificate authorities associated with the remote certificate.</param>
        /// <param name="sslPolicyErrors">One or more errors associated with the remote certificate.</param>
        /// <returns>Returns a boolean value that determines whether the specified certificate is accepted for authentication; true to accept or false to reject.</returns>
        /*
        private static bool ValidateServerCertficate(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
            {
                // Good certificate.
                return true;
            }
            Console.WriteLine(string.Format("SSL certificate error: {0}", sslPolicyErrors));
            try
            {
                using (X509Store store = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                {
                    store.Open(OpenFlags.ReadWrite);
                    store.Add(new X509Certificate2(cert));
                    store.Close();
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(string.Format("SSL certificate add Error: {0}", ex.Message));
            }
            return false;
        }
        */

    }
}
