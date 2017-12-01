# CTlib_csharp
A simple CloudTurbine library written in C#.

CTwriter is currently supported; see the class documentation at https://jpw-erigo.github.io/CTlib_csharp/class_c_tlib_1_1_c_twriter.html

Notes on using CTwriter:

* Supports various types of data: byte arrays, double, float, long, int, short, char.

* Data can optionally be packed and/or ZIP'ed at the block level.

* Timestamps are automatically generated; they can either be in milliseconds or seconds format.

* Old data can optionally be deleted from an existing source folder at startup.

For details on CloudTurbine, see http://www.cloudturbine.com/ and https://github.com/cycronix/cloudturbine.

To compile and use this library:

* Make a local clone of this GitHub repository (https://github.com/jpw-erigo/CTlib_csharp.git)

* Compile the library (I used Microsoft Visual Studio Express 2015 for Windows Desktop); after performing a Release build, the library should be located at "<install_dir>\bin\Release\CTlib.dll".

* Use the simple C# program shown below to try out the library.  Create a new "Console application" project in Visual Studio; add a Reference in the project to the compiled library, CTlib.dll.

A C# example which uses the CTwriter class from the CTlib.dll library is shown below.

```C#
//
// Use the CTwriter class from the C# CTlib library to write data out in
// CloudTurbine format.
//
// The period between samples (in msec) is specified by dataPeriodMsec.
//
// The number of loop iterations between calls to flush is set by
// numLoopsPerBlock; thus, we will call flush approximagely every
// dataPeriodMsec * numLoopsPerBlock msec.
//
// The number of blocks per segment is specified by numBlocksPerSegment.
// Set this to 0 to not have a segment layer.
//
// The desired number of segment folders is specified by numSegmentsToKeep.
// Older segment folders are deleted.  To keep all segment folders, set this
// value to 0.
//
// For information on the CloudTurbine file hierarchy, see
// http://www.cloudturbine.com/structure/.
//

using System;
using System.IO;
using System.Net;
using System.Threading;

namespace CTdemo
{
    class CTdemo
    {
        static byte[] dartmouthImage = null;  // image fetched by a separate thread
        static bool bNewImage = false;        // for synchronized access to the image

        static void Main(string[] args)
        {
            // Settings for data to be written to CT
            String[] ctChanNames = new String[2];
            ctChanNames[0] = "chan1.csv";
            ctChanNames[1] = "chan2.csv";
            double[] ctChanData = new double[2];
            // Settings for the CloudTurbine writer
            int dataPeriodMsec = 100;      // Period between data points
            int numLoopsPerBlock = 10;     // Number of loops to perform between calls to flush
            int numBlocksPerSegment = 10;  // Number of blocks in each segment (0 for no segment layer)
            int numSegmentsToKeep = 3;     // Number of segments to keep, older segment folders are trimmed (0 for no trim, keep all)
            bool bOutputTimesAreMillis = true;
            bool bPack = true;
            bool bZip = true;
            bool bDeleteOldDataAtStartup = true;
            String baseCTOutputFolder = ".";
            if (args.Length > 0)
            {
                baseCTOutputFolder = args[0];
            }
            Console.WriteLine("\nSource output folder = \"{0}\"\n", baseCTOutputFolder);
            CTlib.CTwriter ctw = null;
            try
            {
                ctw = new CTlib.CTwriter(baseCTOutputFolder, numBlocksPerSegment, numSegmentsToKeep, bOutputTimesAreMillis, bPack, bZip, bDeleteOldDataAtStartup);
            }
            catch (Exception e)
            {
                Console.WriteLine("Caught exception trying to create CTwriter:\n{0}", e);
                return;
            }

            // To add a random element to chan2.csv
            Random rnd = new Random();

            // Kick off fetching the first image
            Thread imageThread = new Thread(new ThreadStart(FetchImage));
            imageThread.Start();

            // Write data to the CloudTurbine source
            for (int i = 1; i < 10000; ++i)
            {
                // Image from Dartmouth College webcam
                if (bNewImage)
                {
                    bNewImage = false;
                    ctw.putData("dartmouth.jpg", dartmouthImage);
                    // Launch thread to fetch another image
                    imageThread = new Thread(new ThreadStart(FetchImage));
                    imageThread.Start();
                }
                double testVal = (double)(i % 30);
                ctChanData[0] = 1.0 * testVal;
                ctChanData[1] = Math.Pow(1.1, (double)(i % 30)) + rnd.NextDouble();
                ctw.putData(ctChanNames,ctChanData);
                ctw.putData("double_binary.f64", 2.0 * testVal);
                ctw.putData("double_csv.csv", 3.0 * testVal);
                ctw.putData("int_binary.i32", i);
                ctw.putData("int_text.txt", String.Format("loop index = {0}",i));
                if ((i % numLoopsPerBlock) == 0)
                {
                    // Close the data block by calling flush()
                    Console.Write("\n");
                    try
                    {
                        ctw.flush();
                    }
                    catch (IOException ioe)
                    {
                        Console.WriteLine("\nCaught IOException from CTwriter on flush:\n{0}",ioe);
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

        static void FetchImage()
        {
            dartmouthImage = null;
            // Pace ourselves...
            Thread.Sleep(750);
            using (var webClient = new WebClient())
            {
                dartmouthImage = webClient.DownloadData("http://wc2.dartmouth.edu/jpg/image.jpg");
            }
            bNewImage = true;
        }
    }
}
```

A screenshot of data from this sample application displayed using WebScan/CTweb is shown below:

![](images/CTwriter_demo.png)
