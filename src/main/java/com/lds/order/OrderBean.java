package com.lds.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    /**
     * 订单id和成交金额”作为key，
     * 可以将Map阶段读取到的所有订单数据按照id升序排序，
     * 如果id相同再按照金额降序排序
     */
    private int order_id;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }
    @Override
    public void write(DataOutput out) throws IOException, IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        order_id = in.readInt();
        price = in.readDouble();
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean orderBean) {
        /**
         *         把OrderBean作为key时，order_id、price任一不同则key不相同，
         *         MapTask在读完数据切割好后会调用OrderBean的compareTo来比较，用的快排方法，根据测试文本来看，每个key值都是唯一的
         *         快排完成后数据按id升序按价格降序。
         *         在MapTask完成后，会调用继承的Comparator根据order_id进行分组，order_id相同则分为一组
         */
        if (this.order_id < orderBean.order_id)
            return -1;
        else if (this.order_id > orderBean.order_id)
            return 1;
        else
            return this.price > orderBean.getPrice() ? -1 : 1;
    }
}
