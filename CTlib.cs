
/*
Copyright 2017-2018 Erigo Technologies LLC

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
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;

#if UNITY_5_3_OR_NEWER
// ZIP library used in Unity 3D projects; see ZIP code below for details
using Ionic.Zip;
#endif

/// <summary>
/// A C# implementation of CloudTurbine.
/// </summary>
namespace CTlib
{
    ///
    /// CTwriter
    ///
    /// <summary>
    /// Write CloudTurbine-formatted data.  While not a full one-to-one
    /// implementation of the Java CTwriter API, this C# version provides basic
    /// functionality for storing different types of data.  Data can optionally
    /// be packed and/or ZIP'ed at the block level. Timestamps can either be
    /// represented as milliseconds or seconds since epoch.  All timestamps
    /// are auto-generated (i.e., the user cannot supply timestamps).
    /// </summary>
    ///
    public class CTwriter
    {

        private String baseCTOutputFolder;        // The root folder where the output source is to be written.
        private int numBlocksPerSegment;          // Number of blocks to store in each segment
        private int numSegmentsToKeep;            // Number of full segments to maintain; older segments will be trimmed
        private int currentBlockNum = 0;          // The current block number in the current segment
        // Collection of channel names and their associated data blocks (data is stored as byte arrays in a ChanBlockData object)
        // NOTE: could use ConcurrentDictionary (which is thread safe) but System.Collections.Concurrent isn't supported in Unity at this time
        private IDictionary<string, ChanBlockData> blockData = new Dictionary<string, ChanBlockData>();
        // private long previousTime = -1;        // Used in setStartTimes to check for time not advancing; NOT CURRENTLY USED
        private long startTime = -1;              // Absolute start time for the whole source; this is only set once
        private long segmentStartTime = -1;       // Absolute start time for an individual segment
        private long blockStartTime = -1;         // Absolute start time for an individual block
        private bool bUseMilliseconds = false;    // Output times are milliseconds?
        private bool bPack = false;               // Pack primitive data channels at the block folder level
        private bool bZip = false;                // ZIP data at the block folder level
        private bool bUseTmpZipFiles = false;     // If creating ZIP output files, should we write data to a temporary file (".tmp") and then, when complete, move this file to be ".zip"?  If false (which is the default), stream the complete data set in one dump directly to the output ZIP file.
        private long userSuppliedTimestamp = -1;  // Timestamp supplied by the user in one of the setTime calls
        private long synchronizedTimestamp = -1;  // A timestamp to use across multiple channels (instead of generating a new timestamp for each channel); only used with the putData(string[] channamesI, double[] dataI) and putData(string[] channamesI, float[] dataI) methods
        // List of segment folders
        private List<long> masterSegmentList = new List<long>();
        // character that separates folder segments in a full path name
        private char sepChar = Path.DirectorySeparatorChar;

        // Member variables used when doing asynchronous flush
        private bool bAsync = false;              // Execute flush call in its own thread asynchronously?
        private Thread flushThread = null;        // Thread performing the asynchronous flushes
        private bool doFlushActive = false;       // Is an asynchronous flush currently executing?
        private bool bExitDoFlush = false;        // Exit the asynchronous flush loop?
        private EventWaitHandle _waitHandle = new AutoResetEvent(false);  // To alert the asynchronous flush loop that we want a flush performed.

        // For thread safe coordination to access the data blocks; used when doing asynchronous flushes
        private Object dataLock = new Object();

        ///
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="baseCTOutputFolderI">The root folder where the output source is to be written. This must be at the same level as the application working directory or a sub-directory under it.</param>
        /// <param name="numBlocksPerSegmentI">Number of blocks per segment in the source folder hierarchy.  Use 0 to not include a segment layer.</param>
        /// <param name="numSegmentsToKeepI">When using a segment layer, this specifies the number of full segments to keep. Older segments will be trimmed. Set to 0 to keep all segments.</param>
        /// <param name="bOutputTimesAreMillisI">Output times should be in milliseconds?  Needed if blocks are written (i.e., flush() is called) at a rate greater than 1Hz.</param>
        /// <param name="bPackI">Pack data at the block folder level?  Packed data times are linearly interpolated from the block start time to the time of the final datapoint in the packed channel.</param>
        /// <param name="bZipI">ZIP data at the block folder level?</param>
        /// <param name="bDeleteOldDataAtStartupI">Delete old data from this source at startup?</param>
        /// <param name="bVerifyOutputFolderI">An optional argument, default value is true; if true, verify that baseCTOutputFolderI is at the same level as the application working directory or a sub-directory under it.  Should be set false for HTTP or FTP output types.</param>
        ///
        public CTwriter(String baseCTOutputFolderI, int numBlocksPerSegmentI, int numSegmentsToKeepI, bool bOutputTimesAreMillisI, bool bPackI, bool bZipI, bool bDeleteOldDataAtStartupI, bool bVerifyOutputFolderI = true)
        {
            baseCTOutputFolder = baseCTOutputFolderI;
            if ( (baseCTOutputFolder == null) || (baseCTOutputFolder.Length == 0) )
            {
                throw new Exception("Must supply a base output folder.");
            }
            // If baseCTOutputFolder ends in a directory separator character, remove it (it will be added later)
            if (baseCTOutputFolder.EndsWith(Char.ToString(Path.DirectorySeparatorChar)))
            {
                baseCTOutputFolder = baseCTOutputFolder.Substring(0, baseCTOutputFolder.Length-1);
            }
            numBlocksPerSegment = numBlocksPerSegmentI;
            numSegmentsToKeep = numSegmentsToKeepI;
            bUseMilliseconds = bOutputTimesAreMillisI;
            bPack = bPackI;
            bZip = bZipI;

            if (bVerifyOutputFolderI)
            {
                //
                // Firewall: baseCTOutputFolder must be at the same level as the application working directory or a sub-directory under it
                //
                bool bVerifiedDirectories = false;
                string appWorkingDir = Directory.GetCurrentDirectory();
                // First, test if the application's working directory is the same as the source directory
                string absWorking = Path.GetFullPath(appWorkingDir);
                string absSource = Path.GetFullPath(baseCTOutputFolder);
                if (absWorking.Equals(absSource))
                {
                    // The working directory is the same as the source directory, this is OK
                    bVerifiedDirectories = true;
                }
                else
                {
                    // Second, make sure the source directory is a sub-folder under the application's working directory;
                    // do this by crawling up the source folder hierarchy.
                    // Code to check if a given directory is a sub-folder under another directory was adapted from response on Stack Overflow at
                    //     https://stackoverflow.com/questions/5617320/given-full-path-check-if-path-is-subdirectory-of-some-other-path-or-otherwise
                    // Sample author: BrokenGlass, https://stackoverflow.com/users/329769/brokenglass
                    // License: Stack Overflow content is covered by the Creative Commons license, https://creativecommons.org/licenses/by-sa/3.0/legalcode
                    DirectoryInfo workingDirInfo = new DirectoryInfo(appWorkingDir);
                    DirectoryInfo sourceDirInfo = new DirectoryInfo(baseCTOutputFolder);
                    while (sourceDirInfo.Parent != null)
                    {
                        if (sourceDirInfo.Parent.FullName.Equals(workingDirInfo.FullName))
                        {
                            bVerifiedDirectories = true;
                            break;
                        }
                        else sourceDirInfo = sourceDirInfo.Parent;
                    }
                }
                if (!bVerifiedDirectories)
                {
                    throw new Exception("The source folder must be in or under the application's working directory (i.e., at or under the folder where the application starts)");
                }
            }

            //
            // If requested, delete old/existing data in the source
            //
            if (bDeleteOldDataAtStartupI)
            {
                Console.WriteLine("Deleting old data from source \"{0}\"",baseCTOutputFolder);
                List<string> topFolders = null;
                try
                {
                    topFolders = new List<string>(Directory.GetDirectories(baseCTOutputFolder));
                }
                catch (Exception e)
                {
                    topFolders = null;
                    Console.WriteLine("\tUnable to delete old source data:\n\t\t{0}",e.Message);
                }
                if ((topFolders == null) || (topFolders.Count <= 0))
                {
                    Console.WriteLine("\tNo old data to delete.");
                }
                else
                {
                    foreach (var dir in topFolders)
                    {
                        String folderName = dir.Substring(dir.LastIndexOf(Char.ToString(sepChar)) + 1);
                        // Only delete this folder and its content if it is an integer/long greater than 0
                        try
                        {
                            long folderNameLong = long.Parse(folderName);
                            if (folderNameLong <= 0)
                            {
                                throw new Exception(String.Format("The name of sub-folder \"{0}\" is an integer which is less than or equal to 0.",folderName));
                            }
                            // BE CAREFUL...this does a recursive delete
                            Directory.Delete(dir, true);
                            Console.WriteLine("\tDeleted old data sub-folder \"{0}\"", dir);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("\tSub-folder \"{0}\" will not be deleted:\n\t\t{1}",folderName,e.Message);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Get baseCTOutputFolder.
        /// </summary>
        /// <returns>the base output folder, baseCTOutputFolder</returns>
        public String getBaseCTOutputFolder()
        {
            return baseCTOutputFolder;
        }

        /// <summary>
        /// If writing output ZIP files, should we write data first to
        /// a temporary file (".tmp") and then rename this to the final
        /// ".zip" file?  If false, we write directly to the output
        /// ZIP file using (what is intended to be) a quick stream
        /// operation; this has the possible advantage that an extra
        /// intermediary ".tmp" file isn't created temporarily.
        /// </summary>
        /// <param name="bUseTmpZipFilesI">Write ZIP data first to a temporary file?</param>
        public void UseTmpFileForZipData(bool bUseTmpZipFilesI)
        {
            bUseTmpZipFiles = bUseTmpZipFilesI;
            String classTypeStr = this.GetType().ToString();
            if ( !classTypeStr.Equals("CTwriter") && bUseTmpZipFiles)
            {
                // Only use this method of writing ZIP files when working directly with
                // the base class (i.e., writing local files); not to be used when writing
                // to HTTP, FTP, etc.
                bUseTmpZipFiles = false;
                Console.WriteLine("Writing to a local .tmp file not supported for CTwriters of type {0}; setting value false.",classTypeStr);
            }
        }

        /// <summary>
        /// Based on the given channel name, determine if this channel's data can be packed.
        /// This is based on CTinfo.java methods fileType and wordSize.
        /// </summary>
        /// <param name="channameI">The channel name.</param>
        /// <returns>Return true if the data for this channel can be packed.</returns>
        private static bool canPack(string channameI)
        {
            if (channameI.EndsWith(".bin") || channameI.EndsWith(".jpg") || channameI.EndsWith(".JPG") || channameI.EndsWith(".mp3") || channameI.EndsWith(".txt"))
            {
                return false;
            }
            return true;
        }

        ///
        /// <summary>
        /// Store an additional double data point in each of the specified channels.
        /// </summary>
        /// <param name="channamesI">The names of the channels to which new data will be added.</param>
        /// <param name="dataI">Array containing one new data point per channel.</param>
        /// <exception cref="System.ArgumentException">Thrown if the channel or data arrays are empty of their sizes don't match.</exception>
        /// <seealso cref="putData(string[] channamesI, float[] dataI)"/>
        ///
        public void putData(string[] channamesI, double[] dataI)
        {
            if ((channamesI == null) || (channamesI.Length == 0))
            {
                throw new System.ArgumentException("No channel names were supplied.", "channamesI");
            }
            if ((dataI == null) || (dataI.Length != channamesI.Length))
            {
                throw new System.ArgumentException("Data array is the wrong size.", "dataI");
            }

            lock (dataLock)
            {
                // Use the same timestamp for all channels
                synchronizedTimestamp = getTimestamp();

                // Save data
                for (int i = 0; i < channamesI.Length; ++i)
                {
                    if ((channamesI[i] != null) && (channamesI[i].Length > 0))
                    {
                        putData(channamesI[i], dataI[i]);
                    }
                }

                // Reset synchronizedTimestamp so we stop using it
                synchronizedTimestamp = -1;
            }
        }

        ///
        /// <summary>
        /// Store an additional single-precision float data point in each of the specified channels.
        /// </summary>
        /// <param name="channamesI">The names of the channels to which new data will be added.</param>
        /// <param name="dataI">Array containing one new data point per channel.</param>
        /// <exception cref="System.ArgumentException">Thrown if the channel or data arrays are empty of their sizes don't match.</exception>
        /// <seealso cref="putData(string[] channamesI, double[] dataI)"/>
        ///
        public void putData(string[] channamesI, float[] dataI)
        {
            if ((channamesI == null) || (channamesI.Length == 0))
            {
                throw new System.ArgumentException("No channel names were supplied.", "channamesI");
            }
            if ((dataI == null) || (dataI.Length != channamesI.Length))
            {
                throw new System.ArgumentException("Data array is the wrong size.", "dataI");
            }

            lock (dataLock)
            {
                // Use the same timestamp for all channels
                synchronizedTimestamp = getTimestamp();

                // Save data
                for (int i = 0; i < channamesI.Length; ++i)
                {
                    if ((channamesI[i] != null) && (channamesI[i].Length > 0))
                    {
                        putData(channamesI[i], dataI[i]);
                    }
                }

                // Reset synchronizedTimestamp so we stop using it
                synchronizedTimestamp = -1;
            }
        }

        ///
        /// <summary>
        /// Store a new string in the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, String dataI)
        {
            try
            {
                putData(channameI, Encoding.UTF8.GetBytes(dataI));
            }
            catch (ArgumentException ex)
            {
                throw ex;
            }
        }

        ///
        /// <summary>
        /// Store a double-precision float data value for the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, double dataI)
        {
            try
            {
                if (channameI.EndsWith(".f64"))
                {
                    // store the bytes that make up the double value
                    // note that we store this data in the standard endian-ness of this machine
                    putData(channameI, BitConverter.GetBytes(dataI));
                }
                else
                {
                    // store a string representation of the double
                    string data = dataI.ToString();
                    if (bPack && canPack(channameI))
                    {
                        data = data + ",";
                    }
                    putData(channameI, data);
                }
            }
            catch (ArgumentException ex)
            {
                throw ex;
            }
        }

        ///
        /// <summary>
        /// Store a single-precision float data value for the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, float dataI)
        {
            try
            {
                if (channameI.EndsWith(".f32"))
                {
                    // store the bytes that make up the float value
                    // note that we store this data in the standard endian-ness of this machine
                    putData(channameI, BitConverter.GetBytes(dataI));
                }
                else
                {
                    // store a string representation of the float
                    string data = dataI.ToString();
                    if (bPack && canPack(channameI))
                    {
                        data = data + ",";
                    }
                    putData(channameI, data);
                }
            }
            catch (ArgumentException ex)
            {
                throw ex;
            }
        }

        ///
        /// <summary>
        /// Store a long integer data value for the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, long dataI)
        {
            try
            {
                if (channameI.EndsWith(".i64"))
                {
                    // store the bytes that make up the long value
                    // note that we store this data in the standard endian-ness of this machine
                    putData(channameI, BitConverter.GetBytes(dataI));
                }
                else
                {
                    // store a string representation of the long integer
                    string data = dataI.ToString();
                    if (bPack && canPack(channameI))
                    {
                        data = data + ",";
                    }
                    putData(channameI, data);
                }
            }
            catch (ArgumentException ex)
            {
                throw ex;
            }
        }

        ///
        /// <summary>
        /// Store an integer data value for the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, int dataI)
        {
            try
            {
                if (channameI.EndsWith(".i32"))
                {
                    // store the bytes that make up the integer value
                    // note that we store this data in the standard endian-ness of this machine
                    putData(channameI, BitConverter.GetBytes(dataI));
                }
                else
                {
                    // store a string representation of the integer
                    string data = dataI.ToString();
                    if (bPack && canPack(channameI))
                    {
                        data = data + ",";
                    }
                    putData(channameI, data);
                }
            }
            catch (ArgumentException ex)
            {
                throw ex;
            }
        }

        ///
        /// <summary>
        /// Store a short integer data value for the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, short dataI)
        {
            try
            {
                if (channameI.EndsWith(".i16"))
                {
                    // store the bytes that make up the short integer value
                    // note that we store this data in the standard endian-ness of this machine
                    putData(channameI, BitConverter.GetBytes(dataI));
                }
                else
                {
                    // store a string representation of the short integer
                    string data = dataI.ToString();
                    if (bPack && canPack(channameI))
                    {
                        data = data + ",";
                    }
                    putData(channameI, data);
                }
            }
            catch (ArgumentException ex)
            {
                throw ex;
            }
        }

        ///
        /// <summary>
        /// Store a character data value for the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, char dataI)
        {
            try
            {
                // store a string representation of the character
                string data = dataI.ToString();
                if (bPack && canPack(channameI))
                {
                    data = data + ",";
                }
                putData(channameI, data);
            }
            catch (ArgumentException ex)
            {
                throw ex;
            }
        }

        ///
        /// <summary>
        /// Store binary data array for the given channel.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store.</param>
        /// <exception cref="System.ArgumentException">Thrown if the specified channel name or data array is empty.</exception>
        ///
        public void putData(string channameI, byte[] dataI)
        {
            // FIREWALLS
            if ( (channameI == null) || (channameI.Length == 0) )
            {
                throw new System.ArgumentException("Empty channel name", "channameI");
            }
            if ((dataI == null) || (dataI.Length == 0))
            {
                throw new System.ArgumentException("Data array is empty", "dataI");
            }

            lock (dataLock)
            {
                // Get the timestamp
                long timestamp = getTimestamp();
                
                // Add data to the channel
                if (blockData.ContainsKey(channameI))
                {
                    // Add this data to an existing channel
                    blockData[channameI].add(timestamp, dataI);
                }
                else
                {
                    // Make a new channel data block
                    // Note that we will only pack this data if bPack is true and if the channel name suffix is amenable to packing
                    blockData.Add(channameI, new ChanBlockData(channameI, timestamp, dataI, (bPack && canPack(channameI))));
                }
            }
        }

        ///
        /// <summary>
        /// Get the next timestamp for a data sample.  This is done in one
        /// of two ways:
        /// 
        /// 1. If we are synchronizing time across multiple channels,
        ///    return synchronizedTimestamp.
        /// 
        /// 2. Calculate the time of the next datapoint.  This can be in
        ///    seconds or milliseconds (depending on the value of
        ///    bUseMilliseconds). As needed, we will set startTime,
        ///    segmentStartTime and blockStartTime.
        /// 
        /// NOTE: This method is not thread safe. Calls to this method should be from within a lock block.
        /// </summary>
        /// <returns>The next timestamp.</returns>
        /// 
        private long getTimestamp()
        {

            if (userSuppliedTimestamp > -1)
            {
                // User has supplied their own timestamp; return this value.
                return userSuppliedTimestamp;
            }

            if (synchronizedTimestamp > -1)
            {
                // We are synchronizing time across multiple channels; return this value.
                // This is only used for the putData(string[] channamesI, double[] dataI) and putData(string[] channamesI, float[] dataI) methods.
                return synchronizedTimestamp;
            }

            // Generate epoch timestamp.
            long initialBlockStartTime = blockStartTime;
            long nextTime = -1;
            TimeSpan deltaTime = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            if (bUseMilliseconds)
            {
                nextTime = (long)deltaTime.TotalMilliseconds;
            }
            else
            {
                nextTime = (long)deltaTime.TotalSeconds;
            }

            setStartTimes(nextTime);

            return nextTime;
        }

        /// <summary>
        /// Process the next timestamp.  As needed, set startTime, segmentStartTime and blockStartTime.
        /// 
        /// NOTE: This method is not thread safe. Calls to this method should be from within a lock block.
        /// </summary>
        /// <param name="nextTimeI">The next timestamp to be used.</param>
        private void setStartTimes(long nextTimeI)
        {
            // Check for time not advancing
            // If the user is making several putData calls to adds various channels and is using automatic timestamps,
            // we'll get lots of these warning messages, which isn't very helpful or meaningful; remove this message.
            // if (previousTime >= nextTimeI)
            // {
            //     Console.WriteLine("Warning, time not advancing: previousTime = {0}, nextTime = {1}", previousTime, nextTimeI);
            //     // We could potentially correct this here, but don't implement this now
            //     // nextTime = previousTime+1;
            // }
            // previousTime = nextTimeI;

            if (startTime == -1)
            {
                // Start time for the whole source; this is only set once
                startTime = nextTimeI;
            }
            if ((numBlocksPerSegment > 0) && (segmentStartTime == -1))
            {
                // Start time of the next Segment
                segmentStartTime = nextTimeI;
            }
            if (blockStartTime == -1)
            {
                // Start time of this Block
                blockStartTime = nextTimeI;
            }
        }

        /// <summary>
        /// Set time for subsequent putData().
        /// This method simply calls setTime(<current_time_millis>).
        /// </summary>
        /// <returns>The next timestamp.</returns>
        public long setTime()
        {
            TimeSpan deltaTime = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            long nextTime = (long)deltaTime.TotalMilliseconds;
            setTime(nextTime);
            return nextTime;
        }

        /// <summary>
        /// Set time for subsequent putData().
        /// To switch back to automatic timestamp mode, call this method with the argument equal to -1.
        /// </summary>
        /// <param name="timeI">Timestamp in msec</param>
        public void setTime(long timeI)
        {
            lock (dataLock)
            {
                if (timeI == -1)
                {
                    // Go back to automatic timestamps
                    userSuppliedTimestamp = -1;
                }
                else if (timeI < 0)
                {
                    Console.WriteLine("Timestamp can't be less than 0.");
                }
                else
                {
                    if (bUseMilliseconds)
                    {
                        userSuppliedTimestamp = timeI;
                    }
                    else
                    {
                        userSuppliedTimestamp = (long)(((double)timeI)/1000.0);
                    }
                    setStartTimes(userSuppliedTimestamp);
                }
            }
        }

        /// <summary>
        /// Set time for subsequent putData().
        /// To switch back to automatic timestamp mode, call this method with the argument equal to -1.0.
        /// </summary>
        /// <param name="timeI">Timestamp in sec</param>
        public void setTime(double timeI)
        {
            lock (dataLock)
            {
                if (timeI == -1.0)
                {
                    // Go back to automatic timestamps
                    userSuppliedTimestamp = -1;
                }
                else if (timeI < 0.0)
                {
                    Console.WriteLine("Timestamp can't be less than 0.");
                }
                else
                {
                    if (bUseMilliseconds)
                    {
                        userSuppliedTimestamp = (long)(timeI*1000.0);
                    }
                    else
                    {
                        userSuppliedTimestamp = (long)(timeI);
                    }
                    setStartTimes(userSuppliedTimestamp);
                }
            }
        }

        /// <summary>
        /// Specify whether flushes should be executed asynchronously.
        /// </summary>
        /// <param name="bAsyncI">Should flush calls be asynchronous?</param>
        public void setAsync(bool bAsyncI)
        {
            if (bAsyncI != bAsync)
            {
                // User is requesting a change
                if (bAsync)
                {
                    // We are currently executing flushes asynchronously;
                    // if we are in the middle of a flush, have that finish before proceeding.
                    finishAndKillFlushThread();
                }
                else
                {
                    // We are currently doing synchronous flushes; change to asynchronous.
                    bExitDoFlush = false;
                    doFlushActive = false;
                    flushThread = new Thread(doFlushContinuous);
                    flushThread.Start();
                }
            }
            bAsync = bAsyncI;
            if (bAsync)
            {
                Console.WriteLine("Using asynchronous flush");
            }
            else
            {
                Console.WriteLine("Using synchronous flush");
            }
        }

        /// <summary>
        /// Call doFlush() continuously in a loop.
        /// This method should be executed in a separate thread.
        /// The execution loop in this method is under the control of an EventWaitHandle:
        /// the user calls flush() and inside this flush() method the flush thread is
        /// woken up to perform the flush.
        /// </summary>
        private void doFlushContinuous()
        {
            while (!bExitDoFlush)
            {
                _waitHandle.WaitOne(); // Wait for notification
                // Console.WriteLine("Call flush for " + baseCTOutputFolder);
                doFlush();
                doFlushActive = false;
            }
            // Console.WriteLine("Exiting asynchronous flush loop for " + baseCTOutputFolder);
        }

        ///
        /// <summary>
        /// Write out all data that has been queued up for this block.
        /// </summary>
        /// 
        public void flush()
        {
            if (!bAsync)
            {
                // SYNCHRONOUS
                doFlush();
            }
            else if (bAsync && !doFlushActive)
            {
                // ASYNCHRONOUS
                // Note that we only do the flush if there is no current flush in process.
                doFlushActive = true;
                _waitHandle.Set();    // Wake up the thread to do the flush.
            }
            else if (bAsync)
            {
                // Must be that a flush is currently in process; ignore this flush request.
                // Console.WriteLine("Flush currently in process; ignoring flush request.");
            }
        }

        ///
        /// <summary>
        /// This method does the real work of doing the flush.
        /// 
        /// This method is thread safe, which is needed when CTlib is operating in async mode.
        /// Being thread safe is achieved by making local copies of the blockData and
        /// blockStartTime variables. If we didn't use local copies of these variables and we
        /// were operating in async mode, then both doFlush and putData would be updating
        /// these variables at the same time.
        /// 
        /// The folder hierarchy in which the data files reside is made up of the following parts:
        /// 1. base folder name (given to the CTwriter constructor)
        /// 2. source start time (absolute epoch time of the first data point for the entire source)
        /// 3. [optional] segment start time (relative to the source start time)
        /// 4. block start time (relative to either the source start time or the relative segment start time)
        /// 5. channel data files are in point folders, whose times are relative to the block start time
        /// </summary>
        /// 
        private void doFlush()
        {
            if (blockData.Count == 0)
            {
                // Console.WriteLine("No data to flush, just return");
                return;
            }

            // For thread safety, make local copy of the needed block data and timestamps
            // NOTE: could use ConcurrentDictionary (which is thread safe) but System.Collections.Concurrent isn't supported in Unity at this time
            IDictionary<string, ChanBlockData> local_blockData = new Dictionary<string, ChanBlockData>();
            // Times used in constructing the output data folders
            long segmentStartTimeRel;
            long blockStartTimeRel;
            // In the "lock" block we will use blockStartTime, but elsewhere in this method we will use local_blockStartTime
            long local_blockStartTime;
            lock (dataLock)
            {
                local_blockStartTime = blockStartTime;
                // copy ChanBlockData
                foreach (string channame in blockData.Keys)
                {
                    String local_channame = String.Copy(channame);
                    ChanBlockData cbd_orig = blockData[local_channame];
                    local_blockData.Add(local_channame, new ChanBlockData(cbd_orig));
                }
                // Times used in constructing the output data folders
                segmentStartTimeRel = segmentStartTime - startTime;
                blockStartTimeRel = blockStartTime - startTime;
                if (numBlocksPerSegment > 0)
                {
                    // We are using Segment layer
                    blockStartTimeRel = blockStartTime - segmentStartTime;
                }
                // Reset data and block start time
                // blockData = new Dictionary<string, ChanBlockData>();
                blockData.Clear();
                blockStartTime = -1;

                if (numBlocksPerSegment > 0)
                {
                    // We are using segments
                    ++currentBlockNum;
                    if (currentBlockNum == numBlocksPerSegment)
                    {
                        // Start a new segment
                        currentBlockNum = 0;
                        segmentStartTime = -1;
                    }
                }
            }

            //
            // Write out data for all channels
            // 2 cases: (1) regular files or (2) ZIP files (each block is ZIP'ed)
            //
            if (!bZip)
            {
                // Write regular/non-compressed files
                foreach (string channame in local_blockData.Keys)
                {
                    ChanBlockData cbd = local_blockData[channame];
                    // iterate over the data samples in this channel
                    for (int i = 0; i < cbd.timestamps.Count; ++i)
                    {
                        long timestamp = cbd.timestamps[i];
                        byte[] data = cbd.data[i];
                        // Create the output folder
                        long pointTimeRel = timestamp - local_blockStartTime;
                        String directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + pointTimeRel.ToString() + sepChar;
                        if (numBlocksPerSegment > 0)
                        {
                            // We are using Segment layer
                            directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + segmentStartTimeRel.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + pointTimeRel.ToString() + sepChar;
                        }
                        writeToStream(directoryName, channame, data);
                    }
                }
            }
            else
            {
                //
                // Code found below which creates a ZIP file using the ZipArchive class is based on the following:
                // 1. Example code from the ZipArchive Class documentation from Microsoft available at
                //        https://msdn.microsoft.com/en-us/library/system.io.compression.ziparchive(v=vs.110).aspx
                //    License: Microsoft Limited Public License, available as "Exhibit B" at
                //        https://msdn.microsoft.com/en-us/cc300389
                //        (This license is reproduced in the NOTICE file associated with the CTlib/C# software.)
                // 2. Stack Overflow post found at
                //        https://stackoverflow.com/questions/40175391/invalid-zip-file-after-creating-it-with-system-io-compression
                //    Sample authors:
                //        César Lourenço, https://stackoverflow.com/users/5243419/c%c3%a9sar-louren%c3%a7o
                //        Michal Hainc, https://stackoverflow.com/users/970973/michal-hainc
                //    License: Stack Overflow content is covered by the Creative Commons license, https://creativecommons.org/licenses/by-sa/3.0/legalcode
                //

                // Write each block of data to a separate ZIP file
                // Create the destination directory where the ZIP file will go
                String zipDir = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar;
                if (numBlocksPerSegment > 0)
                {
                    // We are using Segment layer
                    zipDir = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + segmentStartTimeRel.ToString() + sepChar;
                }
                // Only use the ".tmp" method if this is a CTwriter; if this is a derived class
                // (such as CThttp) then don't use this method.
                bool bIsBaseClass = false;
                if (this.GetType().ToString().Equals("CTwriter"))
                {
                    bIsBaseClass = true;
                }
                if (bUseTmpZipFiles && bIsBaseClass) // NOTE: This method should only be used for creating ZIP when using the base class, not for writing to HTTP, FTP, etc.
                {
                    // To avoid CT sink applications from catching the ZIP files
                    // when they are only partially written, write the ZIP as
                    // a temporary (".tmp") file first and then rename it as
                    // a ZIP file.  Both files will be in the standard CT
                    // folder hierarchy.
                    System.IO.Directory.CreateDirectory(zipDir);
                    String fileNameNoSuffix = zipDir + blockStartTimeRel.ToString();
#if UNITY_5_3_OR_NEWER
                    // Create ZIP files in a Unity game application
                    // As of 2017-12-06, Unity supports the .NET 4.6 API, but they
                    // don't include support for ZipArchive.  In place of this, we
                    // use the DotNetZip Ionic.Zip library.  The original code for
                    // this library is at http://dotnetzip.codeplex.com/, but using
                    // Ionic.Zip.dll from this site causes "IBM437 not supported"
                    // errors (a user's program will run fine from the Unity editor
                    // but exceptions are thrown when running the .exe).  Two ways
                    // to fix this problem:
                    // 1. See the solution at https://answers.unity.com/questions/17870/whats-the-best-way-to-implement-file-compression.html;
                    //    need to "copy the I18N*.dll files to your Assets folder".
                    // 2. Use the compiled binary at https://github.com/r2d2rigo/dotnetzip-for-unity;
                    //    this version is custom built for use in Unity and it
                    //    fixes the "IBM437" exceptions.  The "License.Combined.rtf"
                    //    license file at this GitHub repo is a convenient combined
                    //    file which includes all the needed licenses.
                    // Thus, we use the Ionic.Zip library from https://github.com/r2d2rigo/dotnetzip-for-unity
                    using (ZipFile archive = new ZipFile(fileNameNoSuffix + ".tmp"))
                    {
                        foreach (string channame in local_blockData.Keys)
                        {
                            ChanBlockData cbd = local_blockData[channame];
                            // iterate over the data samples in this channel
                            for (int i = 0; i < cbd.timestamps.Count; ++i)
                            {
                                long timestamp = cbd.timestamps[i];
                                byte[] data = cbd.data[i];
                                long pointTimeRel = timestamp - local_blockStartTime;
                                // IMPORTANT: Use the forward slash, "/", to separate file paths in the ZIP file, regardless of what platform we are running on
                                //archive.AddDirectory(pointTimeRel.ToString());
                                archive.AddEntry(pointTimeRel.ToString() + "/" + channame, data);
                            }
                        }
                        archive.Save();
                    }
#else
                    // Zip code using the standard .NET ZipArchive class, write to File
                    using (FileStream hZip = new FileStream(fileNameNoSuffix + ".tmp", FileMode.CreateNew))
                    {
                        using (ZipArchive archive = new ZipArchive(hZip, ZipArchiveMode.Create, true))
                        {
                            foreach (string channame in local_blockData.Keys)
                            {
                                ChanBlockData cbd = local_blockData[channame];
                                // iterate over the data samples in this channel
                                for (int i = 0; i < cbd.timestamps.Count; ++i)
                                {
                                    long timestamp = cbd.timestamps[i];
                                    byte[] data = cbd.data[i];
                                    long pointTimeRel = timestamp - local_blockStartTime;
                                    // IMPORTANT: Use the forward slash, "/", to separate file paths in the ZIP file, regardless of what platform we are running on
                                    ZipArchiveEntry zipEntry = archive.CreateEntry(pointTimeRel.ToString() + "/" + channame);
                                    using (BinaryWriter writer = new BinaryWriter(zipEntry.Open()))
                                    {
                                        writer.Write(data);
                                        writer.Close();  // Since this is in a "using" block, not sure we need the explicit call to Close()
                                    }
                                }
                            }
                        }
                    }
#endif
                    // Give the temporary file its final name
                    File.Move(fileNameNoSuffix + ".tmp", fileNameNoSuffix + ".zip");
                }
                else
                {
                    // Collect data in a MemoryStream first and then dump it
                    // all at once directly to the output ZIP file.
                    using (MemoryStream memOutputStream = new MemoryStream())
                    {
#if UNITY_5_3_OR_NEWER
                        // Zip code using the Ionic.Zip library; see above for details
                        using (ZipFile archive = new ZipFile())
                        {
                            foreach (string channame in local_blockData.Keys)
                            {
                                ChanBlockData cbd = local_blockData[channame];
                                // iterate over the data samples in this channel
                                for (int i = 0; i < cbd.timestamps.Count; ++i)
                                {
                                    long timestamp = cbd.timestamps[i];
                                    byte[] data = cbd.data[i];
                                    long pointTimeRel = timestamp - local_blockStartTime;
                                    // IMPORTANT: Use the forward slash, "/", to separate file paths in the ZIP file, regardless of what platform we are running on
                                    //archive.AddDirectory(pointTimeRel.ToString());
                                    archive.AddEntry(pointTimeRel.ToString() + "/" + channame, data);
                                }
                            }
                            archive.Save(memOutputStream);
                        }
#else
                        // Zip code using the standard .NET ZipArchive class; write to MemoryStream
                        using (ZipArchive archive = new ZipArchive(memOutputStream, ZipArchiveMode.Create, true))
                        {
                            foreach (string channame in local_blockData.Keys)
                            {
                                ChanBlockData cbd = local_blockData[channame];
                                // iterate over the data samples in this channel
                                for (int i = 0; i < cbd.timestamps.Count; ++i)
                                {
                                    long timestamp = cbd.timestamps[i];
                                    byte[] data = cbd.data[i];
                                    long pointTimeRel = timestamp - local_blockStartTime;
                                    // IMPORTANT: Use the forward slash, "/", to separate file paths in the ZIP file, regardless of what platform we are running on
                                    ZipArchiveEntry zipEntry = archive.CreateEntry(pointTimeRel.ToString() + "/" + channame);
                                    using (BinaryWriter writer = new BinaryWriter(zipEntry.Open()))
                                    {
                                        writer.Write(data);
                                        writer.Close();  // Since this is in a "using" block, not sure we need the explicit call to Close()
                                    }
                                }
                            }
                        }
#endif
                        byte[] zipData = memOutputStream.ToArray();
                        writeToStream(zipDir, blockStartTimeRel.ToString() + ".zip", zipData);
                    }
                }
            }

            // Reset data and block start time
            // THIS IS NOW DONE ABOVE IN THE "lock" BLOCK
            // blockData.Clear();
            // blockStartTime = -1;

            // See if it is time to switch to a new Segment folder or trim/delete old segment folders.
            if (numBlocksPerSegment > 0)
            {
                // We are using segments
                // THIS IS NOW DONE ABOVE IN THE "lock" BLOCK
                // ++currentBlockNum;
                // if (currentBlockNum == numBlocksPerSegment)
                // {
                //     // Start a new segment
                //     currentBlockNum = 0;
                //     segmentStartTime = -1;
                // }
                if (numSegmentsToKeep > 0)
                {
                    // Trim old segment folders
                    Boolean bNewFolder = false;
                    // Update our list of segment folders
                    String dirPath = baseCTOutputFolder + sepChar + startTime.ToString();
                    // To stay .NET 3.5 compatible (eg, compatible with Unity 5.5) don't use EnumerateDirectories method
                    // List<string> dirs = new List<string>(Directory.EnumerateDirectories(dirPath));
                    List<string> dirs = new List<string>(Directory.GetDirectories(dirPath));
                    foreach (var dir in dirs)
                    {
                        String folderName = dir.Substring(dir.LastIndexOf(Char.ToString(sepChar)) + 1);
                        // Store the segment folder names as numbers, not strings, so they get sorted correctly
                        long folderNameLong = long.Parse(folderName);
                        if (!masterSegmentList.Contains(folderNameLong))
                        {
                            // This is a new segment folder; store it
                            bNewFolder = true;
                            masterSegmentList.Add(folderNameLong);
                        }
                    }
                    // Only need to consider trimming if a new folder was added
                    if (bNewFolder)
                    {
                        // A new folder is in the list, so we need to re-sort
                        masterSegmentList.Sort();
                        // Console.WriteLine("Sorted list:");
                        // foreach (var folderNum in masterSegmentList)
                        // {
                        //     Console.WriteLine("{0}", folderNum);
                        // }
                        // Trim to maintain desired number of segments
                        // Note that numSegmentsToKeep is the number of *full* segments to keep;
                        // we will keep this number of segments plus the partial segment folder
                        // we are currently writing to.
                        if (masterSegmentList.Count > (numSegmentsToKeep + 1))
                        {
                            int numToTrim = masterSegmentList.Count - (numSegmentsToKeep + 1);
                            for (int i = 0; i < numToTrim; ++i)
                            {
                                // Each time through this loop, remove the oldest entry (at index 0) from the list
                                long folderToDeleteLong = masterSegmentList[0];
                                masterSegmentList.RemoveAt(0);
                                masterSegmentList.Sort();
                                // Remove this folder
                                String dirToDelete = dirPath + sepChar + folderToDeleteLong.ToString();
                                Console.WriteLine("Source {0}, delete segment folder {1}", baseCTOutputFolder, dirToDelete);
                                try
                                {
                                    // BE CAREFUL...this does a recursive delete
                                    Directory.Delete(dirToDelete, true);
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Unable to delete folder {0} due to {1}", dirToDelete, e.Message);
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Low-level method to write data to the channel.
        /// </summary>
        /// <param name="outputDirI"></param>
        /// <param name="chanNameI"></param>
        /// <param name="dataI"></param>
        protected virtual void writeToStream(String outputDirI, String chanNameI, byte[] dataI)
        {
            System.IO.Directory.CreateDirectory(outputDirI);
            // Write out binary data to the channel file in this folder
            File.WriteAllBytes(outputDirI + chanNameI, dataI);
        }

        /// <summary>
        /// Wait for current flush to finish, perform another flush (to flush currently waiting data)
        /// and then have the asynchronous flush thread exit.
        /// </summary>
        private void finishAndKillFlushThread()
        {
            if (!bAsync || (flushThread == null))
            {
                // Flush isn't running in a separate thread; just return.
                return;
            }
            waitForFlushDone();
            if (flushThread != null)
            {
                // Do a final flush
                // After this flush, have the flush thread exit
                bExitDoFlush = true;
                flush();
                waitForFlushDone();
                flushThread = null;
            }
        }

        ///
        /// <summary>
        /// Sleepy loop (up to a max number of iterations) waiting for the current flush to finish.
        /// If flush did not finish during that time, we kill the flush thread and set variable
        /// flushThread equal to null.
        /// </summary>
        /// 
        private void waitForFlushDone()
        {
            if (!bAsync || (flushThread == null))
            {
                // Flush isn't running in a separate thread; just return.
                return;
            }
            int maxNumIters = 100;
            while (doFlushActive && (maxNumIters > 0))
            {
                System.Threading.Thread.Sleep(100);
                --maxNumIters;
            }
            if (doFlushActive)
            {
                // There is still an active flush going on; kill the thread
                Console.WriteLine("Source {0}, flush isn't exiting, kill it.", baseCTOutputFolder);
                try
                {
                    flushThread.Interrupt();
                    flushThread.Abort();
                }
                catch (Exception)
                {
                    // Nothing to do
                }
                flushThread = null;
                doFlushActive = false;
            }
        }

        ///
        /// <summary>
        /// Close the source.
        /// Flush any remaining data to the output source.
        /// </summary>
        /// 
        public virtual void close()
        {
            Console.WriteLine("Close source " + baseCTOutputFolder);
            if (bAsync)
            {
                finishAndKillFlushThread();
            }
            else
            {
                flush();
            }
        }

        ///
        /// ChanBlockData
        /// 
        /// <summary>
        /// Store data for an entire block for a single channel.
        /// </summary>
        /// 
        private class ChanBlockData
        {
            // the channel name
            public string channame = null;

            // list containing all of the timestamps for this channel over a block;
            // when packing data, will only contain a single entry
            public List<long> timestamps = null;

            // list containing all of the data samples for this channel over a block;
            // when packing data, will only contain a single entry
            public List<byte[]> data = null;

            // Pack data for this channel?
            // This is a channel-specific value; in other words, the user may want to
            // pack the entire source, but not all channels are amenable to packing
            // (it depends on the channel suffix).
            private bool bPack = false;

            ///
            /// <summary>
            /// Constructor for the ChanBlockData class
            /// </summary>
            /// <param name="channameI">Channel name</param>
            /// <param name="timestampI">Timestamp to apply to this data sample.</param>
            /// <param name="dataI">Initial data point</param>
            /// <param name="bPackI">Should the data for this channel be packed (i.e., appended into a single byte array)?</param>
            /// 
            public ChanBlockData(string channameI, long timestampI, byte[] dataI, bool bPackI)
            {
                if ((channameI == null) || (channameI.Trim().Length == 0))
                {
                    throw new Exception("The given channel name is empty");
                }
                else if ((dataI == null) || (dataI.Length == 0))
                {
                    throw new Exception("The given data array is empty");
                }
                channame = channameI;
                data = new List<byte[]>();
                data.Add(dataI);
                timestamps = new List<long>();
                timestamps.Add(timestampI);
                bPack = bPackI;
            }

            /// <summary>
            /// Constructor for the ChanBlockData class; copy existing data from the given ChanBlockData structure to initialize this new ChanBlockData object.
            /// </summary>
            /// <param name="origCBD">The existing ChanBlockData object that will be copied.</param>
            public ChanBlockData(ChanBlockData origCBD)
            {
                channame = String.Copy(origCBD.channame);
                timestamps = new List<long>(origCBD.timestamps);
                data = new List<byte[]>(origCBD.data);
                bPack = origCBD.bPack;
            }

            /// <summary>
            /// Store an additional data point.
            /// </summary>
            /// <param name="timestampI">Timestamp to apply to this data sample.</param>
            /// <param name="dataI">The data to add</param>
            public void add(long timestampI, byte[] dataI)
            {
                if ((dataI == null) || (dataI.Length == 0))
                {
                    throw new Exception("ChanBlockData: ERROR: The given data array is empty");
                }
                if (!bPack)
                {
                    // Add the new data and timestamp
                    data.Add(dataI);
                    timestamps.Add(timestampI);
                }
                else
                {
                    // CHECK: Should only have one entry in the time and data lists
                    if ((data.Count != 1) || (timestamps.Count != 1))
                    {
                        throw new Exception("ChanBlockData: ERROR: packed data should only have one entry in the data and timestamps lists");
                    }
                    // Append data to the existing byte array
                    data[0] = appendData(dataI);
                    // Replace the timestamp
                    timestamps[0] = timestampI;
                }
            }

            /// <summary>
            /// Append new data to the existing packed channel data.
            /// Code to combine byte arrays copied from the following Stack Overflow page:
            ///     https://stackoverflow.com/questions/415291/best-way-to-combine-two-or-more-byte-arrays-in-c-sharp
            /// Sample authors:
            ///     Matt Davis, https://stackoverflow.com/users/51170/matt-davis
            ///     Peter Mortensen, https://stackoverflow.com/users/63550/peter-mortensen
            ///     Jon Skeet, https://stackoverflow.com/users/22656/jon-skeet
            /// License: Stack Overflow content is covered by the Creative Commons license, https://creativecommons.org/licenses/by-sa/3.0/legalcode
            /// </summary>
            /// <param name="newData">The new data to add.</param>
            /// <returns>The combined data array.</returns>
            private byte[] appendData(byte[] newData)
            {
                byte[] origData = data[0];
                byte[] ret = new byte[origData.Length + newData.Length];
                Buffer.BlockCopy(origData, 0, ret, 0, origData.Length);
                Buffer.BlockCopy(newData, 0, ret, origData.Length, newData.Length);
                return ret;
            }

        } // end class ChanBlockData

    } // end class CTwriter

} // end namespace CTlib
