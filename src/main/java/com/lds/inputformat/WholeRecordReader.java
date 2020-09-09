package com.lds.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    private Configuration conf;
    private FileSplit split;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress){
         //定义缓冲区
            byte[] contents = new byte[(int) split.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;

            try{
                //获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(conf);

                //读取数据
                fis = fs.open(path);

                //读取文件内容
                IOUtils.readFully(fis,contents,0,contents.length);

                //输出文件内容
                value.set(contents,0,contents.length);

                //获取文件路径及名称
                String name = split.getPath().toString();

                //设置输出的key
                key.set(name);

            }catch (Exception e){

            }finally {
                IOUtils.closeStream(fis);
            }
            isProgress = false;
            return true;
        }

        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
