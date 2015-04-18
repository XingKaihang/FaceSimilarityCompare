import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.util.Iterator;
import java.util.ArrayList;

import java.io.IOException;
import java.util.List;
import java.io.FileOutputStream;
import java.io.File;
import java.io.*;


public class FaceDetect extends Configured implements Tool {

    public static class FaceDetectMapper extends Mapper<ImageHeader, FloatImage, Text, Text> {


        public void map(ImageHeader key, FloatImage value, Context context) throws IOException, InterruptedException 
        {
            // Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
            if (value != null) 
            {

            
                ImageEncoder iencoder;
                String path;

                // Determine file type
                String ext = "";
                if (key.getImageType() == ImageType.JPEG_IMAGE) {
                    ext = ".jpg";
                    iencoder = new JPEGImageUtil();
                } else if (key.getImageType() == ImageType.PNG_IMAGE) {
                    ext = ".png";
                    iencoder = new PNGImageUtil();
                } else {
                System.err.println("Unsupported image type, skipping image.");
                  return;
                }

                //set save path and save name

                path = "/home/hadoop/face/" + value.hex() + ext;
                File file = new File(path);
                file.createNewFile();

                FileOutputStream os = new FileOutputStream(file);
                iencoder.encodeImage(value,key,os);
                os.close();
                


                String command = "br -algorithm FaceRecognition -compare ~/jay/jay.jpg " + path;
                String[] commandAndArgs2 = new String[]{"/bin/sh", "-c", command};
                 // execute my command
                String lastline = "";
                String currentline = "";
                try {

                    // using the Runtime exec method:
                    Process p = Runtime.getRuntime().exec(commandAndArgs2);

                    BufferedReader stdInput = new BufferedReader(new
                         InputStreamReader(p.getInputStream()));

                    // read the output from the command
                    while((currentline = stdInput.readLine()) != null){
                        lastline = currentline;
                      }
                }
                catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }

                // Emit record to reducer
                context.write(new Text("jay.jpg"), new Text(path + "," + lastline));

                file.delete();

            }//if

        } // map()

    } // FaceDetectMapper


    public static class FaceDetectReducer extends Reducer<Text, Text, Text, Text> 
    {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            double maxValue = 0;
            String photoID = "";
            for(Text tempString : values)
            {
                String[] fields = tempString.toString().split(",");
                double temp_value = Double.parseDouble(fields[1]);
                if(maxValue < temp_value)
                {
                    maxValue = temp_value;
                    photoID = fields[0];
                }
            }
            context.write(new Text(key), new Text(photoID + "," + Double.toString(maxValue)));
 
        } // reduce()

    } // FaceDetectReducer



    public int run(String[] args) throws Exception 
    {
       
        // Check input arguments
        if (args.length != 2) 
        {
            System.out.println("Usage: FaceDetect <image hib> <output directory>");
            System.exit(0);
        }
        Path outputpath = new Path(args[1]);
        Path inputpath = new Path(args[0]);


        // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        // Set input format class which parses the input HIB and spawns map tasks
        job.setInputFormatClass(ImageBundleInputFormat.class);
        // Set the driver, mapper, and reducer classes which express the computation
        job.setJarByClass(FaceDetect.class);
        job.setMapperClass(FaceDetectMapper.class);
        job.setReducerClass(FaceDetectReducer.class);
        // Set the types for the key/value pairs passed to/from map and reduce layers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths on the HDFS


        FileOutputFormat.setOutputPath(job, outputpath);
        FileInputFormat.setInputPaths(job, inputpath);
        // Execute the MapReduce job and block until it complets
        boolean success = job.waitForCompletion(true);

        // Return success or failure
        return success ? 0 : 1;
        
    }


    public static void main(String[] args) throws Exception 
    {
        ToolRunner.run(new FaceDetect(), args);
        System.exit(0);
    }

}
