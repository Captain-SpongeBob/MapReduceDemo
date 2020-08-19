package com.lds.WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Hello world!
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * keyin:mr框架读取一行文本的偏移量 valuein：读取一行文本的内容 keyout:输出数据的key valueout：输出数据的value
     */
    private Text keyout = new Text();
    private IntWritable valueout = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] words = line.split(" ");
        //输出
        for (String word : words) {
            keyout.set(word);
            context.write(keyout, valueout);
        }
    }
}
