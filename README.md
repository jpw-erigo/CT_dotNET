# CTlib_csharp
A simple version of the CloudTurbine library written in C#.  CTwriter (the only currently implemented class) writes floating-point data in CloudTurbine format.

CTwriter class documentation is available at https://jpw-erigo.github.io/CTlib_csharp/class_c_tlib_1_1_c_twriter.html

Notes on using CTwriter:

* An array of channel names is given to the class constructor.  A corresponding data array must be given to each putData() call.  For example, if channel "foo.csv" is at index=3 in the channel name array given to the constructor, then data for channel "foo.csv" must be at index=3 in the data array given to putData().  The same number of entries must be supplied in the channel name array as in the data arrays.

* Only double-precision floating point data is currently supported.

* Timestamps are automatically supplied; they can either be in milliseconds or seconds, as specified by a boolean argument to the constructor.

For details on CloudTurbine, see http://www.cloudturbine.com/ and https://github.com/cycronix/cloudturbine.

To compile and use this library:

* Make a local clone of this GitHub repository (https://github.com/jpw-erigo/CTlib_csharp.git)

* Compile the library (I used Microsoft Visual Studio Express 2015 for Windows Desktop); after performing a Release build, the library should be located at "<install_dir>\bin\Release\CTlib.dll".

* Use the simple C# program shown below to try out the library.  Create a new "Console application" project in Visual Studio; add a Reference in the project to the compiled library, CTlib.dll.

A C# example which uses the CTwriter class from the CTlib.dll library is shown below.

```C#
//
// Use the CTwriter class from the CTlib.dll library to write data out in
// CloudTurbine format.
//
// This sample program writes out 2 channels:
//   o "chan1.csv" (contains an incrementing index)
//   o "chan2.csv" (waveform with random noise)
//
// The period between samples (in msec) is specified by dataPeriodMsec.
//
// Each output file ("chan1.csv" and "chan2.csv") contains the number of
// points specified by numPtsPerCTFile in CSV format.
//
// Each output CloudTurbine "block" contains one output file per channel,
// i.e., one "chan1.csv" file and one "chan2.csv" file. The number of
// blocks per segment is specified by numBlocksPerSegment.
//
// The number of segment folders maintained in the output source is
// specified by totNumSegments; older segment folders are trimmed.
//
// For information on the CloudTurbine file hierarchy, see
// http://www.cloudturbine.com/structure/.
//

using System;
using System.IO;
using System.Threading;

namespace CTdemo
{
    class CTdemo
    {
        static void Main(string[] args)
        {
            // Configure the CloudTurbine writer
            int numCTChans = 2;
            String[] ctChanNames = new String[numCTChans];
            ctChanNames[0] = "chan1.csv";
            ctChanNames[1] = "chan2.csv";
            double[] ctChanData = new double[numCTChans];
            int dataPeriodMsec = 100;      // Period between data points
            int numPtsPerCTFile = 10;      // Number of points per channel per file
            int numBlocksPerSegment = 10;  // Number of blocks in each segment (set to 0 for no segment layer)
            int totNumSegments = 3;        // Total number of segments to keep (set to 0 to keep everything)
            String baseCTOutputFolder = ".\\CTdata\\CTdemo\\";
            CTlib.CTwriter ctw =
                new CTlib.CTwriter(baseCTOutputFolder, ctChanNames, numBlocksPerSegment, totNumSegments, true);

            // To add a random element to chan2.csv
            Random rnd = new Random();

            // Write data to the CloudTurbine source
            for (int i = 0; i < 10000; ++i)
            {
                ctChanData[0] = (double)i;
                ctChanData[1] = Math.Pow(1.1, (double)(i % 30)) + rnd.NextDouble();
                ctw.putData(ctChanData);
                if ((i % numPtsPerCTFile) == 0)
                {
                    Console.Write("\n");
                    // Close the data block by calling flush()
                    try
                    {
                        ctw.flush();
                    }
                    catch (IOException ioe)
                    {
                        Console.WriteLine("\nCaught IOException from CTwriter on flush");
                        if (ioe.Source != null)
                        {
                            Console.WriteLine("IOException source: {0}", ioe.Source);
                        }
                    }
                }
                Console.Write(".");
                Thread.Sleep(dataPeriodMsec);
            }

            // Close the CloudTurbine writer
            try
            {
                ctw.close();
            }
            catch (IOException ioe)
            {
                Console.WriteLine("\nCaught IOException from CT library on close");
                if (ioe.Source != null)
                {
                    Console.WriteLine("IOException source: {0}", ioe.Source);
                }
            }
        }
    }
}
```

A screenshot of data from this sample application displayed using WebScan/CTweb is shown below:

![](images/CTwriter_demo.png)
