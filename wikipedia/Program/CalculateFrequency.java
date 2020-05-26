import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalculateFrequency
{
	 public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
    	Configuration conf = new Configuration();
    	Job job1 = Job.getInstance(conf);
    	job1.setJarByClass(CalculateFrequency.class);
        job1.setJobName("finding_relative_frequency");

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_o"));

        job1.setMapperClass(CalMap.class);
        job1.setReducerClass(CalReduce.class);
        job1.setCombinerClass(SplitterReducer.class);
        job1.setPartitionerClass(SplitterPartition.class);
        job1.setNumReduceTasks(3);

        job1.setOutputKeyClass(Splitter.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.waitForCompletion(true);

        Configuration conf1 = new Configuration();
    	  Job job2 = Job.getInstance(conf1);
    	  job2.setJarByClass(CalculateFrequency.class);
    	  job2.setJobName("finding_relative_frequency");

    	  job2.setSortComparatorClass(Shuffle.class);
        FileInputFormat.addInputPath(job2, new Path("temp_o"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));


        job2.setMapperClass(CalMap2.class);
        job2.setReducerClass(CalReduce2.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(Splitter.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}