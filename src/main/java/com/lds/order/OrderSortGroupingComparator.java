package com.lds.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortGroupingComparator extends WritableComparator {

    protected OrderSortGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        //只要id相同就认为是相同的key
        if (aBean.getOrder_id() > bBean.getOrder_id())
            return 1;
        else if (aBean.getOrder_id() < bBean.getOrder_id())
            return -1;
        else
            return 0;
    }
}
