
//
// CT_dotNET
//
// This class mimics some very simple CTlib.CTwriter functionality.
//
// An array of channel names is given to the Constructor.  There must be a
// one-to-one correspondance of these channels in the data array given to
// putData().  Calling flush() writes data to the current Block and
// then closes the current Block.
//

using System;
using System.Collections.Generic;
using System.IO;

namespace CT_dotNET
{
    public class CT_dotNET
    {

        String baseCTOutputFolder;
        String[] chanNames;
        int numChans;
        int numBlocksPerSegment;
        int currentBlockNum = 0;
        // Packed data gets staged in a temporary file and then moved to the real CT
        List<double>[] ctData;
        long startTimeSec = -1;         // Absolute start time for the whole source
        long segmentStartTimeSec = -1;  // Absolute start time for an individual segment
        long blockStartTimeSec = -1;    // Absolute start time for an individual block
        long lastDataPtTimeSec = -1;    // Absolute time of the latest data point

        //
        // Constructor
        //
        public CT_dotNET(String baseCTOutputFolderI, String[] chanNamesI, int numBlocksPerSegmentI)
        {
            baseCTOutputFolder = baseCTOutputFolderI;
            chanNames = chanNamesI;
            numBlocksPerSegment = numBlocksPerSegmentI;

            numChans = chanNames.Length;

            ctData = new List<double>[numChans];
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i] = new List<double>();
            }
        }

        //
        // Add data to the Lists.  There must be a one-to-one correspondance between the entries
        // in this data array and the channel name array given to the Constructor.
        //
        public void putData(double[] dataI)
        {
            if ((dataI == null) || (dataI.Length != numChans))
            {
                throw new System.ArgumentException("Data array is the wrong size", "dataI");
            }
            lastDataPtTimeSec = (long)((DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds);
            if (startTimeSec == -1)
            {
                // Start time for the whole source
                startTimeSec = lastDataPtTimeSec;
            }
            if ((numBlocksPerSegment > 0) && (segmentStartTimeSec == -1))
            {
                // Start time of the next Segment
                segmentStartTimeSec = lastDataPtTimeSec;
            }
            if (ctData[0].Count == 0)
            {
                // Start time of this Block
                blockStartTimeSec = lastDataPtTimeSec;
            }
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i].Add(dataI[i]);
            }
        }

        //
        // Write data out to one Block
        // The Block will contain one packed data file
        // Name of the file is made up of the following parts:
        // 1. base folder name (given to the constructor and stored in baseCTOutputFolder)
        // 2. source start time (absolute)
        // 3. [optional] segment start time (relative to the source start time)
        // 3. block start time (relative to either the source start time or the relative segment start time)
        // 4. block duration
        //
        public void flush()
        {
            if (ctData[0].Count == 0)
            {
                throw new System.IO.IOException("No data ready to flush");
            }

            // Construct a folder to contain the packed data file
            long blockDuration = lastDataPtTimeSec - blockStartTimeSec;
            long segmentStartTimeRel = segmentStartTimeSec - startTimeSec;
            long blockStartTimeRel = blockStartTimeSec - startTimeSec;
            if (numBlocksPerSegment > 0)
            {
                // We are using Segment layer
                blockStartTimeRel = blockStartTimeSec - segmentStartTimeSec;
            }
            String directoryName = baseCTOutputFolder + startTimeSec.ToString() + "\\" + blockStartTimeRel.ToString() + "\\" + blockDuration.ToString() + "\\";
            if (numBlocksPerSegment > 0)
            {
                // We are using Segment layer
                directoryName = baseCTOutputFolder + startTimeSec.ToString() + "\\" + segmentStartTimeRel.ToString() + "\\" + blockStartTimeRel.ToString() + "\\" + blockDuration.ToString() + "\\";
            }
            System.IO.Directory.CreateDirectory(directoryName);

            // Write one packed CSV data file for each channel
            for (int i = 0; i < numChans; ++i)
            {
                StreamWriter ctFile = new StreamWriter(File.Open(directoryName + chanNames[i], FileMode.Create));
                foreach (double dataPt in ctData[i])
                {
                    ctFile.Write("{0:F2},", dataPt);
                }
                ctFile.Close();
            }

            // Clear out the data arrays
            for (int i = 0; i < numChans; ++i)
            {
                ctData[i].Clear();
            }

            // See if it is time to switch to a new Segment folder
            if (numBlocksPerSegment > 0)
            {
                ++currentBlockNum;
                if (currentBlockNum == numBlocksPerSegment)
                {
                    currentBlockNum = 0;
                    segmentStartTimeSec = -1;
                }
            }
        }

        //
        // Close this CT writer
        // For now, just flush
        //
        public void close()
        {
            flush();
        }
    }
}
