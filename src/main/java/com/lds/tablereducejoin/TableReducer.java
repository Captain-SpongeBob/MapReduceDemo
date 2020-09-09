package com.lds.tablereducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //存储订单的集合
        ArrayList<TableBean> list = new ArrayList<>();
        TableBean pdBean = new TableBean();
        for (TableBean value : values) {
            if ("order".equals(value.getFlag())){
                //拷贝传递过来的bean的属性到集合中
                //准备bean对象
                TableBean bean = new TableBean();
                try{
                    BeanUtils.copyProperties(bean,value);
                    list.add(bean);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }else {
                try{
                    // 拷贝传递过来的产品表到内存中
                    BeanUtils.copyProperties(pdBean, value);

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        // 3 表的拼接
        for (TableBean orderBean : list) {
            orderBean.setPname(pdBean.getPname());
            context.write(orderBean,NullWritable.get());
        }
    }
}
