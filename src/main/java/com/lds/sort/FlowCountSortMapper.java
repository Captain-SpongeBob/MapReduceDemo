package com.lds.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
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
       String phonenum = fields[0];
       //
       long upFlow = Long.parseLong(fields[fields.length - 3]);
       long downFlow = Long.parseLong(fields[fields.length - 2]);
       
       //
        k.set(phonenum);
        v.set(upFlow,downFlow);//输入上下行数据量初始化总数据量

        //按照bean的sum值进行排序
        context.write(v, k);
    }
}