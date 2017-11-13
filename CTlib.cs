﻿
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
        String[] chanNames;
        int numChans;
        int numBlocksPerSegment;
        int totNumSegments;
        int currentBlockNum = 0;
        List<double>[] ctData;          // Packed data gets staged in a List and then written to CT file when flush() is called
        long startTime = -1;            // Absolute start time for the whole source
        long segmentStartTime = -1;     // Absolute start time for an individual segment
        long blockStartTime = -1;       // Absolute start time for an individual block
        long lastDataPtTime = -1;       // Absolute time of the latest data point
        bool bUseMilliseconds = false;  // Output times are milliseconds?
        List<long> masterSegmentList = new List<long>();  // List of segment folders

        ///
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="baseCTOutputFolderI">The root folder where the output source is to be written.</param>
        /// <param name="chanNamesI">Array of channel names.</param>
        /// <param name="numBlocksPerSegmentI">Number of blocks per segment in the source folder hierarchy.  Use 0 to not include a segment layer.</param>
        /// <param name="totNumSegmentsI">When using a segment layer, this specifies the total number of segments to maintain. Older segments will be trimmed. Only used if value is greater than 1 (i.e., to activate trim, the min value is 2; at a minimum, we will keep the currently active segment as well as the previous one).</param>
        /// <param name="bOutputTimesAreMillisI">Output times should be in milliseconds?  Needed if blocks are written (i.e., flush() is called) at a rate greater than 1Hz.</param>
        ///
        public CTwriter(String baseCTOutputFolderI, String[] chanNamesI, int numBlocksPerSegmentI, int totNumSegmentsI, bool bOutputTimesAreMillisI)
        {
            baseCTOutputFolder = baseCTOutputFolderI;
            // If baseCTOutputFolder ends in a directory separator character, remove it (it will be added later)
            if (baseCTOutputFolder.EndsWith(Char.ToString(Path.DirectorySeparatorChar)))
            {
                baseCTOutputFolder = baseCTOutputFolder.Substring(0, baseCTOutputFolder.Length-1);
            }
            chanNames = chanNamesI;
            numBlocksPerSegment = numBlocksPerSegmentI;
            totNumSegments = totNumSegmentsI;
            bUseMilliseconds = bOutputTimesAreMillisI;

            numChans = chanNames.Length;

            ctData = new List<double>[numChans];
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i] = new List<double>();
            }
        }

        ///
        /// <summary>
        /// Store additional data for each channel.  There must be a one-to-one
        /// correspondance between the entries in the given data array and the
        /// channel name array that was provided to the Constructor.  As a minimal
        /// check, these arrays must be the same size or else this method will
        /// throw an exception.
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
            // Calculate the new time in either seconds or milliseconds
            TimeSpan deltaTime = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            if (bUseMilliseconds)
            {
                lastDataPtTime = (long)deltaTime.TotalMilliseconds;
            }
            else
            {
                lastDataPtTime = (long)deltaTime.TotalSeconds;
            }
            if (startTime == -1)
            {
                // Start time for the whole source
                startTime = lastDataPtTime;
            }
            if ((numBlocksPerSegment > 0) && (segmentStartTime == -1))
            {
                // Start time of the next Segment
                segmentStartTime = lastDataPtTime;
            }
            if (ctData[0].Count == 0)
            {
                // Start time of this Block
                blockStartTime = lastDataPtTime;
            }

            // Save data
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i].Add(dataI[i]);
            }
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
        /// 3. block start time (relative to either the source start time or the relative segment start time)
        /// 4. block duration
        /// </summary>
        /// <exception cref="System.IO.IOException">Thrown when there is no data ready to flush to file.</exception>
        /// 
        public void flush()
        {
            if (ctData[0].Count == 0)
            {
                throw new System.IO.IOException("No data ready to flush");
            }

            // Construct a folder to contain the packed data file
            long blockDuration = lastDataPtTime - blockStartTime;
            long segmentStartTimeRel = segmentStartTime - startTime;
            long blockStartTimeRel = blockStartTime - startTime;
            if (numBlocksPerSegment > 0)
            {
                // We are using Segment layer
                blockStartTimeRel = blockStartTime - segmentStartTime;
            }
            char sepChar = Path.DirectorySeparatorChar;
            String directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + blockDuration.ToString() + sepChar;
            if (numBlocksPerSegment > 0)
            {
                // We are using Segment layer
                directoryName = baseCTOutputFolder + sepChar + startTime.ToString() + sepChar + segmentStartTimeRel.ToString() + sepChar + blockStartTimeRel.ToString() + sepChar + blockDuration.ToString() + sepChar;
            }
            System.IO.Directory.CreateDirectory(directoryName);

            // Write one packed CSV data file for each channel
            for (int i = 0; i < numChans; ++i)
            {
                StreamWriter ctFile = new StreamWriter(File.Open(directoryName + chanNames[i], FileMode.Create));
                foreach (double dataPt in ctData[i])
                {
                    ctFile.Write("{0:G},", dataPt);
                }
                ctFile.Close();
            }

            // Clear out the data arrays
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i].Clear();
            }

            // See if it is time to switch to a new Segment folder or trim/delete old segment folders.
            if (numBlocksPerSegment > 0)
            {
                // We are using segments
                ++currentBlockNum;
                if (currentBlockNum == numBlocksPerSegment)
                {
                    currentBlockNum = 0;
                    segmentStartTime = -1;
                }
                if (totNumSegments > 1)
                {
                    // User wants to trim/delete old segments
                    Boolean bNewFolder = false;
                    // Update our list of segment folders
                    String dirPath = baseCTOutputFolder + sepChar + startTime.ToString();
                    List<string> dirs = new List<string>(Directory.EnumerateDirectories(dirPath));
                    foreach (var dir in dirs)
                    {
                        String folderName = dir.Substring(dir.LastIndexOf(Char.ToString(sepChar)) + 1);
                        long folderNameLong = long.Parse(folderName);
                        if (!masterSegmentList.Contains(folderNameLong))
                        {
                            // This is a new segment folder; store it
                            bNewFolder = true;
                            masterSegmentList.Add(folderNameLong);
                        }
                    }
                    // Only need to consider trimming if there has been a new folder added
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
                        if (masterSegmentList.Count > totNumSegments)
                        {
                            int numToTrim = masterSegmentList.Count - totNumSegments;
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
    } // end class CTwriter
} // end namespace CTlib