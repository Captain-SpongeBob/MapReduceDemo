package com.lds.NLine;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /**
     * 对每个单词的数量进行统计，每三行放入一个切片中
     */
    private Text k = new Text();
    private LongWritable v = new LongWritable(1);

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splited = line.split(" ");
        //把每行单词切割
        for (int i = 0; i < splited.length; i++) {
            k.set(splited[i]);
            context.write(k, v);
        }
    }
}