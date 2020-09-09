package com.lds.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderSortMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private OrderBean keyout = new OrderBean();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split("\t");
        keyout.setOrder_id(Integer.parseInt(fileds[0]));
        keyout.setPrice(Double.parseDouble(fileds[2]));
        context.write(keyout,NullWritable.get());
    }
}
