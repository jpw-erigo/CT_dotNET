
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
using System.IO.Compression;
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
        // List of CTbinary objects, for storing byte array channel data
        private IDictionary<string, ChanBlockData> blockData = new Dictionary<string, ChanBlockData>();
        private long startTime = -1;              // Absolute start time for the whole source
        private long segmentStartTime = -1;       // Absolute start time for an individual segment
        private long blockStartTime = -1;         // Absolute start time for an individual block
        private bool bUseMilliseconds = false;    // Output times are milliseconds?
        private bool bPack = false;               // Pack primitive data channels at the block folder level
        private bool bZip = false;                // ZIP data at the block folder level
        private long synchronizedTimestamp = -1;  // A timestamp to use across multiple channels (instead of generating a new timestamp for each channel)
        // List of segment folders
        private List<long> masterSegmentList = new List<long>();
        // character that separates folder segments in a full path name
        private char sepChar = Path.DirectorySeparatorChar;

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
        ///
        public CTwriter(String baseCTOutputFolderI, int numBlocksPerSegmentI, int numSegmentsToKeepI, bool bOutputTimesAreMillisI, bool bPackI, bool bZipI, bool bDeleteOldDataAtStartupI)
        {
            baseCTOutputFolder = baseCTOutputFolderI;
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

            // Use the same timestamp for all channels
            synchronizedTimestamp = getTimestamp();

            // Save data
            for (int i=0; i<channamesI.Length; ++i)
            {
                if ( (channamesI[i] != null) && (channamesI[i].Length > 0) )
                {
                    putData(channamesI[i], dataI[i]);
                }
            }

            // Reset synchronizedTimestamp so we stop using it
            synchronizedTimestamp = -1;
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
                throw;
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
                throw;
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
                throw;
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
                throw;
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
                throw;
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
                throw;
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
                throw;
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
        /// </summary>
        /// <returns>The next timestamp.</returns>
        /// 
        private long getTimestamp()
        {

            if (synchronizedTimestamp > 0)
            {
                // We are synchronizing time across multiple channels; use this value
                return synchronizedTimestamp;
            }

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
        /// Write out all data that has been queued up for this block.
        /// 
        /// The folder hierarchy in which the data files reside is made up of the following parts:
        /// 1. base folder name (given to the CTwriter constructor)
        /// 2. source start time (absolute epoch time of the first data point for the entire source)
        /// 3. [optional] segment start time (relative to the source start time)
        /// 4. block start time (relative to either the source start time or the relative segment start time)
        /// 5. channel data files are in point folders, whose times are relative to the block start time
        /// </summary>
        /// <exception cref="System.IO.IOException">Thrown when there is no data to flush to file.</exception>
        /// 
        public void flush()
        {
            if (blockData.Count == 0)
            {
                throw new System.IO.IOException("No data ready to flush");
            }

            //
            // Times used in constructing the output data folders
            //
            long segmentStartTimeRel = segmentStartTime - startTime;
            long blockStartTimeRel = blockStartTime - startTime;
            if (numBlocksPerSegment > 0)
            {
                // We are using Segment layer
                blockStartTimeRel = blockStartTime - segmentStartTime;
            }

            //
            // Write out data for all channels
            // 2 cases: writing to regular files or creating a ZIP file
            //
            if (!bZip)
            {
                // Write regular/non-compressed files
                foreach (string channame in blockData.Keys)
                {
                    ChanBlockData cbd = blockData[channame];
                    // iterate over the data samples in this channel
                    for (int i = 0; i < cbd.timestamps.Count; ++i)
                    {
                        long timestamp = cbd.timestamps[i];
                        byte[] data = cbd.data[i];
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
                        File.WriteAllBytes(directoryName + channame, data);
                    }
                }
            }
            else
            {
                // All block data is written to one compressed ZIP file
                // The ZipArchive code found below was largely copied from
                //     https://msdn.microsoft.com/en-us/library/system.io.compression.ziparchive(v=vs.110).aspx
                // I've seen a somewhat alternative solution using MemoryStream in place of FileStream at different sites; for example
                //     https://stackoverflow.com/questions/40175391/invalid-zip-file-after-creating-it-with-system-io-compression
                // To avoid CT sink applications from catching the ZIP files when they are only partially written,
                // what we do below is write the ZIP file to a temporary location and then move the file to its
                // final location within the CT hierarchy.
                // Temporary location where the ZIP file will be written
                String tempDir = Path.GetTempPath();
                using (FileStream hZip = new FileStream(tempDir + blockStartTimeRel.ToString() + ".zip", FileMode.CreateNew))
                {
                    using (ZipArchive archive = new ZipArchive(hZip, ZipArchiveMode.Create, true))
                    {
                        foreach (string channame in blockData.Keys)
                        {
                            ChanBlockData cbd = blockData[channame];
                            // iterate over the data samples in this channel
                            for (int i = 0; i < cbd.timestamps.Count; ++i)
                            {
                                long timestamp = cbd.timestamps[i];
                                byte[] data = cbd.data[i];
                                long pointTimeRel = timestamp - blockStartTime;
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
                // Create the destination directory where the ZIP file will go
                String zipDir = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar;
                if (numBlocksPerSegment > 0)
                {
                    // We are using Segment layer
                    zipDir = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + segmentStartTimeRel.ToString() + sepChar;
                }
                System.IO.Directory.CreateDirectory(zipDir);
                // Move the ZIP file to the destination directory
                File.Move(tempDir + blockStartTimeRel.ToString() + ".zip", zipDir + blockStartTimeRel.ToString() + ".zip");
            }

            // Reset data and block start time
            blockData.Clear();
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
                    if ( (data.Count != 1) || (timestamps.Count != 1) )
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
            /// Code copied from https://stackoverflow.com/questions/415291/best-way-to-combine-two-or-more-byte-arrays-in-c-sharp
            /// </summary>
            /// <param name="newData">The new data to add.</param>
            /// <returns>The combined data array.</returns>
            public byte[] appendData(byte[] newData)
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
