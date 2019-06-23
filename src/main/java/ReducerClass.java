/**
 * Big Data Project - MapReduce using CSV
 * Reducer class
 *
 * Muhamad Aldy B.
 * Source: https://www.guru99.com/create-your-first-hadoop-program.html
 *
 * This is the reducer class which takes data from mapper class and doing job as reducer. As its name, reducer class
 * will have job to produce set of data by using key which determined by the mapper class. To understand it, imagine
 * that the mapper class already map the set of original data into the keys, for example sets or array. This class
 * will using that data, not the original one, which passed from mapper class to produce new set of data. The new set
 * of data has reducing the amount of original data into small or chunk of data with key as its identifier.
 *
 * In this project, reducer class will have job to counting how many data from the same key collected by the mapper
 * class. The output will produce new data with the key as the stable variable and value of how many that data with
 * same key has been found.
 */


/**
 * Import some libraries
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ReducerClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * This is the method for reducer class which doing reduce job
     *
     * @param t_key             The key was used by the mapper class to mapping the same object
     * @param values            The values which save how many data with the same key
     * @param output            Output of the reducer class that collect the keys and the values
     * @param reporter          Reporter class
     * @throws IOException      Input-Output java exception, throws when takes error while doing input/output files
     */
    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {

        // determine key object and counter variable
        Text key = t_key;
        int counter = 0;

        // as long that the values inside the data being mapped,
        // will counting how many data with the same key
        while (values.hasNext()) {

            // replace type of value with the actual type of our value
            IntWritable value = (IntWritable) values.next();
            counter += value.get();
        }
        output.collect(key, new IntWritable(counter));
    }
}