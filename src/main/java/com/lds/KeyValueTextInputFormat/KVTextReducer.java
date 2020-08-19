package com.lds.KeyValueTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int sum = 0;
    private IntWritable v = new IntWritable();

    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
    for (IntWritable value : values) {
        sum += value.get();
        
    }
    v.set(sum);
    context.write(key, v);
    }    
}