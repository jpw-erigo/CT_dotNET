
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
using System.Collections.Generic;
using System.IO;
using System.Text;

/// <summary>
/// A C# implementation of CloudTurbine.
/// </summary>
namespace CTlib
{
    ///
    /// CTwriter
    ///
    /// <summary>
    /// Write floating-point data out in CloudTurbine format.
    ///
    /// An array of channel names is given to the Constructor.  The same number
    /// of entries must be supplied in the data array given to putData(); there
    /// should be a one-to-one correspondance between the channel name index and
    /// the index in the data array.
    /// 
    /// Only double-precision floating point data is currently supported.
    /// 
    /// Timestamps can either be in milliseconds or seconds, as specified by
    /// the boolean argument to the constructor.
    /// </summary>
    ///
    public class CTwriter
    {

        String baseCTOutputFolder;
        String[] chanNames;             // Array of channel names for the packed data channels
        int numChans;                   // Number of packed data channels (ie, size of chanNames)
        int numBlocksPerSegment;        // Number of blocks to store in each segment
        int numSegmentsToKeep;          // Number of full segments to maintain; older segments will be trimmed
        int currentBlockNum = 0;        // The current block number in the current segment
        List<double>[] ctData;          // Packed double-precision floating point data gets staged in this List and then written to CT file when flush() is called
        List<CTbinary> ctBinary;        // List of CTbinary objects, for storing byte array channel data
        long startTime = -1;            // Absolute start time for the whole source
        long segmentStartTime = -1;     // Absolute start time for an individual segment
        long blockStartTime = -1;       // Absolute start time for an individual block
        long lastPackedDataPtTime = -1; // Absolute time of the latest packed data point
        bool bUseMilliseconds = false;  // Output times are milliseconds?
        List<long> masterSegmentList = new List<long>();  // List of segment folders
        char sepChar = Path.DirectorySeparatorChar;       // character that separates folder segments in a full path name

        ///
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="baseCTOutputFolderI">The root folder where the output source is to be written.</param>
        /// <param name="chanNamesI">Array of channel names.</param>
        /// <param name="numBlocksPerSegmentI">Number of blocks per segment in the source folder hierarchy.  Use 0 to not include a segment layer.</param>
        /// <param name="numSegmentsToKeepI">When using a segment layer, this specifies the number of full segments to keep. Older segments will be trimmed. Set to 0 to keep all segments.</param>
        /// <param name="bOutputTimesAreMillisI">Output times should be in milliseconds?  Needed if blocks are written (i.e., flush() is called) at a rate greater than 1Hz.</param>
        /// <param name="bDeleteOldDataAtStartupI">Delete old data from this source at startup?</param>
        ///
        public CTwriter(String baseCTOutputFolderI, String[] chanNamesI, int numBlocksPerSegmentI, int numSegmentsToKeepI, bool bOutputTimesAreMillisI, bool bDeleteOldDataAtStartupI)
        {
            baseCTOutputFolder = baseCTOutputFolderI;
            // If baseCTOutputFolder ends in a directory separator character, remove it (it will be added later)
            if (baseCTOutputFolder.EndsWith(Char.ToString(Path.DirectorySeparatorChar)))
            {
                baseCTOutputFolder = baseCTOutputFolder.Substring(0, baseCTOutputFolder.Length-1);
            }
            chanNames = chanNamesI;
            numBlocksPerSegment = numBlocksPerSegmentI;
            numSegmentsToKeep = numSegmentsToKeepI;
            bUseMilliseconds = bOutputTimesAreMillisI;

            numChans = 0;
            if (chanNames != null)
            {
                numChans = chanNames.Length;
            }

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
                // This code was copied from https://stackoverflow.com/questions/5617320/given-full-path-check-if-path-is-subdirectory-of-some-other-path-or-otherwise
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

            //
            // Initialize the data lists
            //
            ctData = new List<double>[numChans];
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i] = new List<double>();
            }
            ctBinary = new List<CTbinary>();

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

        ///
        /// <summary>
        /// Store additional data in each of the packed channels.  There must
        /// be a one-to-one correspondance between the entries in the given
        /// data array and the channel name array that was provided to the
        /// Constructor.  As a minimal check, these arrays must be the same
        /// size or else this method will throw an exception.
        /// </summary>
        /// <param name="dataI">Array containing one new data point per channel.</param>
        /// <exception cref="System.ArgumentException">Thrown when the size of the given data array doesn't match the number of channels (as given to the constructor).</exception>
        ///
        public void putData(double[] dataI)
        {
            if ((dataI == null) || (dataI.Length != numChans))
            {
                throw new System.ArgumentException("Data array is the wrong size", "dataI");
            }

            // Update time
            lastPackedDataPtTime = getTimestamp();

            // Save data
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i].Add(dataI[i]);
            }
        }

        ///
        /// <summary>
        /// Store string data for the given channel at the current timestamp.
        /// The given channel name must not match any of the packed data
        /// channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
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
                throw;
            }
        }

        ///
        /// <summary>
        /// Store a double-precision data value for the given channel at the
        /// current timestamp. The given channel name must not match any of
        /// the packed data channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
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
                    putData(channameI, dataI.ToString());
                }
            }
            catch (ArgumentException ex)
            {
                throw;
            }
        }

        ///
        /// <summary>
        /// Store a single-precision data value for the given channel at the
        /// current timestamp. The given channel name must not match any of
        /// the packed data channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
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
                    putData(channameI, dataI.ToString());
                }
            }
            catch (ArgumentException ex)
            {
                throw;
            }
        }

        ///
        /// <summary>
        /// Store a long integer data value for the given channel at the
        /// current timestamp. The given channel name must not match any of
        /// the packed data channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
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
                    // store a string representation of the long
                    putData(channameI, dataI.ToString());
                }
            }
            catch (ArgumentException ex)
            {
                throw;
            }
        }

        ///
        /// <summary>
        /// Store an integer data value for the given channel at the
        /// current timestamp. The given channel name must not match any of
        /// the packed data channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
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
                    putData(channameI, dataI.ToString());
                }
            }
            catch (ArgumentException ex)
            {
                throw;
            }
        }

        ///
        /// <summary>
        /// Store a short integer data value for the given channel at the
        /// current timestamp. The given channel name must not match any of
        /// the packed data channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
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
                    putData(channameI, dataI.ToString());
                }
            }
            catch (ArgumentException ex)
            {
                throw;
            }
        }

        ///
        /// <summary>
        /// Store a character data value for the given channel at the
        /// current timestamp. The given channel name must not match any of
        /// the packed data channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
        /// <exception cref="System.ArgumentException">Re-throws any exception that is thrown by the underlying <see cref="putData(string,byte[])"/> call.</exception>
        ///
        public void putData(string channameI, char dataI)
        {
            try
            {
                // store a string representation of the character
                putData(channameI, dataI.ToString());
            }
            catch (ArgumentException ex)
            {
                throw;
            }
        }

        ///
        /// <summary>
        /// Store binary data array for the given channel at the current timestamp.
        /// The given channel name must not match any of the packed data channels.
        /// </summary>
        /// <param name="channameI">Channel to store the given data in.</param>
        /// <param name="dataI">The data to store at the current timestamp for the given channel name.</param>
        /// <exception cref="System.ArgumentException">Thrown if the specified channel name is empty or it matches one of the packed channel names.</exception>
        ///
        public void putData(string channameI, byte[] dataI)
        {
            if ((dataI == null) || (dataI.Length == 0))
            {
                return;
            }

            if ( (channameI == null) || (channameI.Length == 0) )
            {
                throw new System.ArgumentException("Empty channel name", "channameI");
            }

            // Make sure the given channel name isn't one of the packed data channels
            // (ie, can't be both a packed channel and also a binary byte array channel)
            for (int i=0; i<numChans; ++i)
            {
                if (channameI.Equals(chanNames[i]))
                {
                    throw new System.ArgumentException(String.Format("The given channel name,\"{0}\", matches the name of one of the packed channels"), "channameI");
                }
            }

            ctBinary.Add(new CTbinary(this, channameI, dataI));
        }

        ///
        /// <summary>
        /// Calculate the time of the next datapoint.  This can be
        /// in seconds or milliseconds, as specified by the user.
        /// As needed, we will also set startTime, segmentStartTime
        /// and blockStartTime.
        /// </summary>
        /// <returns>The next timestamp.</returns>
        /// 
        private long getTimestamp()
        {
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
            if (startTime == -1)
            {
                // Start time for the whole source
                startTime = nextTime;
            }
            if ((numBlocksPerSegment > 0) && (segmentStartTime == -1))
            {
                // Start time of the next Segment
                segmentStartTime = nextTime;
            }
            if (blockStartTime == -1)
            {
                // Start time of this Block
                blockStartTime = nextTime;
            }
            return nextTime;
        }

        ///
        /// <summary>
        /// Write data out to one block.  Each block contains one packed CSV data file
        /// per channel.
        /// 
        /// The folder containing these channel data files is made up of the following parts:
        /// 1. base folder name (given to the constructor)
        /// 2. source start time (absolute epoch time)
        /// 3. [optional] segment start time (relative to the source start time)
        /// 4. block start time (relative to either the source start time or the relative segment start time)
        /// 5. block duration
        /// </summary>
        /// <exception cref="System.IO.IOException">Thrown when there is no data ready to flush to file.</exception>
        /// 
        public void flush()
        {

            bool isPackedData = false;
            if ( (numChans > 0) && (ctData[0].Count > 0) )
            {
                isPackedData = true;
            }

            if ( (!isPackedData) && (ctBinary.Count == 0) )
            {
                throw new System.IO.IOException("No data ready to flush");
            }

            //
            // Times used in constructing the output data folders
            //
            long blockDuration = lastPackedDataPtTime - blockStartTime;
            long segmentStartTimeRel = segmentStartTime - startTime;
            long blockStartTimeRel = blockStartTime - startTime;
            if (numBlocksPerSegment > 0)
            {
                // We are using Segment layer
                blockStartTimeRel = blockStartTime - segmentStartTime;
            }

            //
            // Write one packed CSV data file for each packed data channel
            //
            if (isPackedData)
            {
                // First, construct a folder to contain the packed data file
                String directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + blockDuration.ToString() + sepChar;
                if (numBlocksPerSegment > 0)
                {
                    // We are using Segment layer
                    directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + segmentStartTimeRel.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + blockDuration.ToString() + sepChar;
                }
                System.IO.Directory.CreateDirectory(directoryName);
                // Second, write out the packed data
                for (int i = 0; i < numChans; ++i)
                {
                    StreamWriter ctFile = new StreamWriter(File.Open(directoryName + chanNames[i], FileMode.Create));
                    foreach (double dataPt in ctData[i])
                    {
                        ctFile.Write("{0:G},", dataPt);
                    }
                    ctFile.Close();
                }
            }

            //
            // Write out byte array data
            //
            foreach (var ctbin in ctBinary)
            {
                long timestamp = ctbin.timestamp;
                // Create the output folder
                long pointTimeRel = timestamp - blockStartTime;
                String directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + pointTimeRel.ToString() + sepChar;
                if (numBlocksPerSegment > 0)
                {
                    // We are using Segment layer
                    directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + segmentStartTimeRel.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + pointTimeRel.ToString() + sepChar;
                }
                System.IO.Directory.CreateDirectory(directoryName);
                // Write out binary data to the channel file in this new folder
                File.WriteAllBytes(directoryName + ctbin.channame, ctbin.data);
            }

            //
            // Clear out the data lists
            //
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i].Clear();
            }
            ctBinary.Clear();

            // Reset the block start time
            blockStartTime = -1;

            // See if it is time to switch to a new Segment folder or trim/delete old segment folders.
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
                        if (masterSegmentList.Count > (numSegmentsToKeep+1))
                        {
                            int numToTrim = masterSegmentList.Count - (numSegmentsToKeep+1);
                            for (int i=0; i<numToTrim; ++i)
                            {
                                // Each time through this loop, remove the oldest entry (at index 0) from the list
                                long folderToDeleteLong = masterSegmentList[0];
                                masterSegmentList.RemoveAt(0);
                                masterSegmentList.Sort();
                                // Remove this folder
                                String dirToDelete = dirPath + sepChar + folderToDeleteLong.ToString();
                                Console.WriteLine("Delete segment folder {0}", dirToDelete);
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

        ///
        /// <summary>
        /// Flush any remaining data to the output source.
        /// </summary>
        /// 
        public void close()
        {
            flush();
        }

        ///
        /// CTbinary
        /// 
        /// <summary>
        /// This class stores one sample of a binary array.
        /// </summary>
        /// 
        private class CTbinary
        {
            public long timestamp = -1;
            public string channame = null;
            public byte[] data = null;

            ///
            /// <summary>
            /// Constructor for the CTbinary class
            /// </summary>
            /// <param name="ctwI">Reference to the parent CTwriter class</param>
            /// <param name="channameI">Channel name</param>
            /// <param name="dataI">The binary array</param>
            /// 
            public CTbinary(CTwriter ctwI, string channameI, byte[] dataI)
            {
                if ((channameI == null) || (channameI.Trim().Length == 0))
                {
                    throw new Exception("The given channel name is empty");
                }
                else if ((dataI == null) || (dataI.Length == 0))
                {
                    throw new Exception("The given data array is empty");
                }
                else
                {
                    channame = channameI;
                    data = dataI;
                    timestamp = ctwI.getTimestamp();
                }
            }
        } // end class CTbinary

    } // end class CTwriter

} // end namespace CTlib
