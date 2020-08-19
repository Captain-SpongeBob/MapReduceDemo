package com.lds.KeyValueTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {
    private IntWritable v = new IntWritable(1);

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //KVformat      banzi banzhang banhua    k:banzi   v: banzhang banhua
        context.write(key, v);
        
    }
}