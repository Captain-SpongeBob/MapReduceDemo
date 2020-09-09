package com.lds.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/input/inputFlow.txt","d:/output"};

        //获取配置
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定jar位置
        job.setJarByClass(FlowCountDriver.class);

        //指定mapper reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指定mapper  的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定reducer  kv输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定自定义数据分区
        job.setPartitionerClass(ProvincePartitioner.class);

        //定义ReduceTask的数量
        job.setNumReduceTasks(5);

        //指定输入输出文件目录
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs ? 0 : 1);
    
    }
}