package com.lds.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    public void reduce(FlowBean k, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("调用reducer");
        for (Text value : values) {
            context.write(value,k);
        }
    }
}