package com.wanshen.job.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.entity.Orders;
import com.wanshen.job.entity.Products;
import com.wanshen.job.util.kafkaUtil;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import static com.wanshen.job.common.Config.*;

public class test4 {

    private static Row t;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        LinkedBlockingDeque<Orders> orders = new LinkedBlockingDeque<>();
        orders.add(new Orders(1,new Date(new java.util.Date().getTime()),INSERT,new BigDecimal(23),1001,true,new java.util.Date().getTime()));
        orders.add(new Orders(2,new Date(new java.util.Date().getTime()),INSERT,new BigDecimal(24),1002,true,new java.util.Date().getTime()));
        orders.add(new Orders(2,new Date(new java.util.Date().getTime()),DELETE,new BigDecimal(25),1003,true,new java.util.Date().getTime()));
        orders.add(new Orders(1,new Date(new java.util.Date().getTime()),UPDATE,new BigDecimal(25),1111,true,new java.util.Date().getTime()));
        orders.add(new Orders(3,new Date(new java.util.Date().getTime()),INSERT,new BigDecimal(25),1003,true,new java.util.Date().getTime()));
        LinkedBlockingDeque<Products> products = new LinkedBlockingDeque<>();
        products.add(new Products(1,INSERT,"tt",new java.util.Date().getTime()));
        products.add(new Products(3,INSERT,"tt",new java.util.Date().getTime()));
        products.add(new Products(3,UPDATE,"tt",new java.util.Date().getTime()));
        products.add(new Products(2,INSERT,"tt",new java.util.Date().getTime()));
        products.add(new Products(2,DELETE,"tt",new java.util.Date().getTime()));
        products.add(new Products(1,UPDATE,"张三",new java.util.Date().getTime()));
        DataStreamSource<Orders> ordersDataStreamSource = env.fromCollection(orders).setParallelism(1);
        DataStreamSource<Products> productsDataStreamSource = env.fromCollection(products).setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(tableEnv.fromDataStream(ordersDataStreamSource));
        SingleOutputStreamOperator<Row> customerName = rowDataStream.map(t -> {
            String name = t.getField("customerName").toString();
            if (DELETE.equals(name)) {
                t.setKind(RowKind.DELETE);
            }
            if (UPDATE.equals(name)) {
                t.setKind(RowKind.UPDATE_AFTER);
            }
            if (INSERT.equals(name)) {
                t.setKind(RowKind.INSERT);
            }
            return t;
        });

        DataStream<Row> productsDataStream = tableEnv.toChangelogStream(tableEnv.fromDataStream(productsDataStreamSource));
        SingleOutputStreamOperator<Row> productsData = productsDataStream.map(t -> {
            String name = t.getField("name").toString();
            if (DELETE.equals(name)) {
                t.setKind(RowKind.DELETE);
            }
            if (UPDATE.equals(name)) {
                t.setKind(RowKind.UPDATE_AFTER);
            }
            if (INSERT.equals(name)) {
                t.setKind(RowKind.INSERT);
            }
            return t;
        });

        SingleOutputStreamOperator<Orders> order1 = customerName.map(t -> {
            int id =Integer.valueOf(t.getField("orderId").toString()) ;
            String name = t.getField("customerName").toString();
            int productId = Integer.valueOf(t.getField("productId").toString());
            Orders products1 = new Orders();
            products1.setOrderId(id);
            products1.setCustomerName(name);
            products1.setOrderDate(new Date(new java.util.Date().getTime()));
            products1.setCreateTime(new java.util.Date().getTime());
            products1.setProductId(productId);
            return products1;
        });
        SingleOutputStreamOperator<Products> map = productsData.map(t -> {
            int id =Integer.valueOf(t.getField("id").toString()) ;
            String name = t.getField("name").toString();
            Long createTime = Long.valueOf(t.getField("createTime").toString());

            Products products1 = new Products();
            products1.setId(id);
            products1.setName(name);
            products1.setCreateTime(createTime);
            return products1;
        });



//        tableEnv.createTemporaryView("order",customerName, Schema.newBuilder()
//                        .column("orderId", DataTypes.INT())
//                        .column("orderDate", DataTypes.STRING())
//                        .column("customerName", DataTypes.STRING())
//                        .column("price", DataTypes.STRING())
//                        .column("productId", DataTypes.STRING())
//                        .column("createTime", DataTypes.STRING())
//                .build());
        tableEnv.createTemporaryView("product",map);
        tableEnv.createTemporaryView("order",order1);
//        tableEnv.createTemporaryView("order",ordersDataStreamSource);
//        tableEnv.createTemporaryView("product",productsDataStreamSource);



//    tableEnv.executeSql("select * from `order`").print();
//    tableEnv.executeSql("select * from `product`").print();
        Table table = tableEnv.sqlQuery("select  *  from `order` as o left join  `product` as p on o.orderId=p.id");
//        SingleOutputStreamOperator<Row> customerName1 = tableEnv.toDataStream(table).map(t -> {
//            String name = t.getField("customerName").toString();
//            if (DELETE.equals(name)) {
//                t.setKind(RowKind.DELETE);
//            }
//            if (UPDATE.equals(name)) {
//                t.setKind(RowKind.UPDATE_AFTER);
//            }
//            if (INSERT.equals(name)) {
//                t.setKind(RowKind.INSERT);
//            }
//            return t;
//        });
     //   Table table1 = tableEnv.fromDataStream(customerName1);

        DataStream<Row> rowDataStream1 = tableEnv.toChangelogStream(table).map(
                t->{
           String name = t.getField("customerName").toString();
            if (DELETE.equals(name)) {
                t.setKind(RowKind.DELETE);
            }
            if (UPDATE.equals(name)) {
                t.setKind(RowKind.UPDATE_AFTER);
            }
            if (INSERT.equals(name)) {
                t.setKind(RowKind.INSERT);
            }
                    return  t;
                }
        );

     rowDataStream1.print();


      env.execute();

    }
}
