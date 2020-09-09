package com.lds.outputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter<Text, NullWritable> {
    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;


    public FilterRecordWriter(TaskAttemptContext job) {
        FileSystem fs = null;
        try{
            fs = FileSystem.get(job.getConfiguration());

            //创建输出路径
            Path atguiguPath = new Path("d:/output/atguigu.log");
            Path otherPath = new Path("d:/output/other.log");

            //创建输出流
            atguiguOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("atguigu")){
            atguiguOut.writeBytes(key.toString());
        }else {
            otherOut.writeBytes(key.toString());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
