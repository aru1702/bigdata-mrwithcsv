/**
 * Big Data Project - MapReduce using CSV
 * Main Class
 *
 * Muhamad Aldy B.
 * Source: https://www.guru99.com/create-your-first-hadoop-program.html
 *
 * This class is to run the both Mapper Class job and the Reducer Class job.
 */


/**
 * Import some libraries
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class MainClass {

    /**
     * Main Java class
     *
     * This is the main class to be run inside this project. It includes how to start the Map and Reduce Job,
     * using the Hadoop Mapper class method to mapping with our Mapper class, using the Hadoop Reducer class
     * method to reducing with our Reducer class, getting the files and produce the output files, and finish
     * the both job and also determine how to exit the program.
     *
     * @param args          Main arguments, it's basic of main java class
     * @throws Exception    Exception for any error found, throws when any exception been found
     */
    public static void main(String[] args) {

        // create new JobClient
        JobClient my_client = new JobClient();

        // Create a configuration object for the job
        JobConf job_conf = new JobConf(MainClass.class);

        // Set a name of the Job
        job_conf.setJobName("MapReduceCSV");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(MapperClass.class);
        job_conf.setReducerClass(ReducerClass.class);

        // Specify formats of the data type of Input and output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments, 
        // arg[0] = name of input directory on HDFS, and
        // arg[1] =  name of output directory to be created to store the output file.

        // called the input path for file and defined the output path
        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

        my_client.setConf(job_conf);
        try {
            // Run the job 
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}