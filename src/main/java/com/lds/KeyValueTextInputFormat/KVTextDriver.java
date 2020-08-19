package com.lds.KeyValueTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/input/kvinput.txt","d:/output"};
     Configuration configuration = new Configuration();
     configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
     Job job = Job.getInstance(configuration);

     //设置jar包 
     job.setJarByClass(KVTextDriver.class);
     job.setMapperClass(KVTextMapper.class);
     job.setReducerClass(KVTextReducer.class);

     //set mapper
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(IntWritable.class);

     //set reducer  output
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(IntWritable.class);

     job.setInputFormatClass(KeyValueTextInputFormat.class);
     
     FileInputFormat.setInputPaths(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));

     System.out.println(job.waitForCompletion(true));
    }
}   