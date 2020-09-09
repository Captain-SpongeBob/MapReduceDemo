package com.lds.tablereducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    private String name;
    private TableBean tableBean = new TableBean();
    private Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        if (name.startsWith("order")){//订单表单处理
            /**
             * id	    pid	 amount
             * 1001 	01	  1
             */
            tableBean.setOrder_id(fields[0]);
            tableBean.setP_id(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");

            k.set(fields[1]);

        }else {//产品表单处理
            /**
             * pid	pname
             * 01	小米
             * 02	华 为
             */
            tableBean.setOrder_id("");
            tableBean.setP_id(fields[0]);
            tableBean.setAmount(0);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("pd");

            k.set(fields[0]);
        }
        // 3 写出
        context.write(k,tableBean);

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取输入文件切片信息
        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        //获取输入文件名称
        name = fileSplit.getPath().getName();
    }
}
