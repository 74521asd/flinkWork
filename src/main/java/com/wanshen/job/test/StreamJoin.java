package com.wanshen.job.test;

import com.wanshen.job.entity.Orders;
import com.wanshen.job.entity.Products;
import com.wanshen.job.util.kafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.Duration;
import java.util.ArrayList;


public class StreamJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Orders> orders = new ArrayList<>();
        orders.add(new Orders(1,new Date(new java.util.Date().getTime()),"wangwu",new BigDecimal(23),1001,true,new java.util.Date().getTime()));
        orders.add(new Orders(2,new Date(new java.util.Date().getTime()),"wangwu1",new BigDecimal(23),1002,true,new java.util.Date().getTime()));
        orders.add(new Orders(3,new Date(new java.util.Date().getTime()),"wangwu2",new BigDecimal(23),1003,true,new java.util.Date().getTime()));
        ArrayList<Products> products = new ArrayList<>();
        products.add(new Products(1,"zhansan","tt",new java.util.Date().getTime()));
        products.add(new Products(2,"zhansan","tt",new java.util.Date().getTime()));
        products.add(new Products(3,"zhansan","tt",new java.util.Date().getTime()));
        DataStreamSource<Orders> ordersDataStreamSource = env.fromCollection(orders);
        DataStreamSource<Products> productsDataStreamSource = env.fromCollection(products);
        ordersDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Orders>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Orders>() {
            @Override
            public long extractTimestamp(Orders orders, long l) {
                return orders.getCreateTime();
            }
        }));
        productsDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Products>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Products>() {
            @Override
            public long extractTimestamp(Products products, long l) {
                return products.getCreateTime();
            }
        }));

        DataStream<String> full = ordersDataStreamSource.join(productsDataStreamSource)
                .where(t -> t.getCreateTime())
                .equalTo(t -> t.getCreateTime())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.minutes(3000))
                .apply(new JoinFunction<Orders, Products, String>() {
                    @Override
                    public String join(Orders orders, Products products) throws Exception {
                        String o = null;
                        return o;
                    }
                });


    }




}
