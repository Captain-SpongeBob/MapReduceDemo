package com.lds.BeanWritable;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean v = new FlowBean();
    private Text k = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //split
        String[] fields = line.split("\t");

        //封装对象
       //手机号
       String phonenum = fields[1];
       //
       long upFlow = Long.parseLong(fields[fields.length - 3]);
       long downFlow = Long.parseLong(fields[fields.length - 2]);
       
       //
        k.set(phonenum);
        v.set(upFlow,downFlow);
        //
        context.write(k, v);
    }
}