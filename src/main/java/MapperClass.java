/**
 * Big Data Project - MapReduce using CSV
 * Mapper class
 *
 * Muhamad Aldy B.
 * Source: https://www.guru99.com/create-your-first-hadoop-program.html
 *
 * This is the mapper class which mapping the original data into the set of data using the same key as the identifier.
 * Mapper class will get the data from file has been imported, looking into the key will be used, and collecting the data
 * with the same key (the mapping job). It's more like having the set of data, like arrays, where the key is the name of
 * array while the values inside them is the data being mapped. Furthermore, the set of data will passed into the reducer
 * class for taking the reducer job.
 *
 * In this project, mapper class will determine which column being used as key. After that the data inside that column
 * will be mapped with each same value as the key.
 */


/**
 * Import some libraries
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MapperClass extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {

    // initialize the field variable
    private final static IntWritable one = new IntWritable(1);
    private final static int COLUMN_USED = 4;
    private final static String CSV_SEPARATOR = ",";

    /**
     * This is the mapper method which mapping the values inside the column has been defined
     *
     * @param key               The key will be used for mapper class
     * @param value             The data from the original file
     * @param output            Output of the mapper class
     * @param reporter          Reporter class
     * @throws IOException      Input-Output java exception, throws when takes error while doing input/output files
     */
    public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

        // initiate the variable
        String valueString = value.toString();

        // split the data with CSV_SEPARATOR
        String[] columnData = valueString.split(CSV_SEPARATOR);

        // collect the data with defined column
        output.collect(new Text(columnData[COLUMN_USED]), one);
    }
}