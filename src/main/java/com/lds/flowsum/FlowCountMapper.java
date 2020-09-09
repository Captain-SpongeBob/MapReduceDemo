package com.lds.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text,FlowBean> {
    FlowBean v = new FlowBean();
    Text k = new Text();


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
        v.set(upFlow,downFlow);
        k.set(phonenum);

        //
        context.write(k, v);
    }
}