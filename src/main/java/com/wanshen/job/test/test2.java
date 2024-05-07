package com.wanshen.job.test;

import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.common.Tools;
import com.wanshen.job.entity.Orders;
import com.wanshen.job.entity.Products;
import com.wanshen.job.sink.StarRocksJdbcSink;
import lombok.extern.slf4j.Slf4j;
import netscape.javascript.JSObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.*;

import static com.wanshen.job.common.Tools.getInsertSql;
import static com.wanshen.job.common.Tools.rowToJsonObject;

@Slf4j
public class test2 {
    public static void main(String[] args) throws Exception {
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
//        DataStreamSource<Products> productsDataStreamSource = env.fromCollection(products);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(ordersDataStreamSource);
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table);
        HashMap<String, String> map = new HashMap<>();
        map.put("orderStatus","order_status");
        map.put("orderId","order_id");
        map.put("orderDate","order_date");
        map.put("customerName","customer_name");
        map.put("productId","product_id");
        HashMap<String, String> map1 = new HashMap<>();
        map1.put("orderDate","String");



        rowDataStream.flatMap(new FlatMapFunction<Row, JSONObject>() {
            @Override
            public void flatMap(Row row, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = rowToJsonObject(row);
                System.out.println("jsonObject = " + jsonObject);
                jsonObject.remove("createTime");
                jsonObject.put("sql",getInsertSql(jsonObject, "Orders", "com.wanshen.job.entity.Orders", map));
                log.info("生成的json对象{}",jsonObject);
                collector.collect(jsonObject);
            }
        }).addSink(new StarRocksJdbcSink());
        DataStream<Row> rowDataStream1 = tableEnv.toChangelogStream(table);




        env.execute();


    }
}
