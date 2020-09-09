package com.lds.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    //电话号码做key
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String preNum = text.toString().substring(0,3);
        int partiotion = 4;
        // 判断是哪个省份
        if (preNum.equals("139")){
            partiotion = 0;
        }else if (preNum.equals("135")){
            partiotion = 1;
        }else if(preNum.equals("178")){
            partiotion = 2;
        }else if (preNum.equals("165")){
            partiotion = 3;
        }
        return partiotion;
    }
}
