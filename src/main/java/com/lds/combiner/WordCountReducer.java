package com.lds.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int sum;
    private IntWritable v = new IntWritable();
    
	@Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //sum
        for (IntWritable count : values) {
            sum += count.get();
        }
        //output
        v.set(sum);
        context.write(key, v);
    }
    
}