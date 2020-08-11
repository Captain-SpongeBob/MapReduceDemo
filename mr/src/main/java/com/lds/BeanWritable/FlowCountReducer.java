package com.lds.BeanWritable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_dpwnFlow = 0;

        for (FlowBean v : values) {
            sum_upFlow += v.getUpFlow();
            sum_dpwnFlow += v.getDownFlow();
        }

        //封装
        FlowBean floeBean = new FlowBean(sum_upFlow,sum_dpwnFlow);

        //
        context.write(key, floeBean);
    }
}