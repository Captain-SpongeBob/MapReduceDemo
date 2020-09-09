package com.lds.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowConterSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/input/phone_data.txt","d:/output"};

        //获取配置
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定jar位置
        job.setJarByClass(FlowConterSortDriver.class);

        //指定mapper reducer
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        //指定mapper  的输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定reducer  kv输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定输入输出文件目录
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs ? 0 : 1);
    
    }
}