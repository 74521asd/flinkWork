package com.wanshen.job.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.entity.Orders;
import com.wanshen.job.entity.Products;
import com.wanshen.job.entity.Shipments;
import com.wanshen.job.sink.MyRetractStreamTableSink;
import com.wanshen.job.util.kafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import static com.wanshen.job.common.Config.*;
import static com.wanshen.job.common.MappingConfig.ORDERS_TABLE;
import static com.wanshen.job.common.MappingConfig.PRODUCTS_TABLE;

/**
 * 三表接入Kafka关联查询写入starRocks
 */
@Slf4j
public class ThreeTableJoin {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("rest.port","8081");
        configuration.setString(RestOptions.BIND_PORT,"8081");
//        configuration.setString(WebOptions.LOG_PATH,"D:/log/log.log");
//        configuration.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY,"D:/log/log.log");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> Shipments = env.addSource(kafkaUtil.getKafkaConsumer(KAFKA_TOPIC_SIT_ORACLE, "c40", DEV_KAFKA_IP));
        DataStreamSource<String> source = env.addSource(kafkaUtil.getKafkaConsumer(KAFKA_TOPIC_SIT_TLSQL, "b39", DEV_KAFKA_IP));
        OutputTag<String> ProductsOutputTag = new OutputTag<String>(PRODUCTS_TABLE, Types.STRING);
        //todo 分流
        SingleOutputStreamOperator<Orders> orderStream = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                              //  System.out.println("value = " + value);
                                log.info("value ={} " , value);
                try {
                    String tableName = JSONObject.parseObject(value).getString("tableName");
                    //todo 数据在此分流，
                    if (PRODUCTS_TABLE.equals(tableName)){
                        context.output(ProductsOutputTag, value);

                    }
                    if (ORDERS_TABLE.equals(tableName)) {
                        collector.collect(value);
                    }
                    else {
                        log.info("异常数据{}",value);
                    }
                } catch (Exception e) {
                   log.error("JSON解析异常");
                }
            }
            //todo 主流数据过滤清洗转换
        }).flatMap(new FlatMapFunction<String, Orders>() {
            @Override
            public void flatMap(String value, Collector<Orders> collector) throws Exception {

                try {
                    String operType = JSONObject.parseObject(value).getString("operType");
                    JSONObject jsonObject = JSON.parseObject(value);
                    String bean = "";
                    if (UPDATE.equals(operType)) {
                        bean = jsonObject.getString(AFTER);
                    }
                    if (INSERT.equals(operType)) {
                        bean = jsonObject.getString("columnInfo");
                    }
                    Orders orders = JSON.parseObject(bean, Orders.class);
                    collector.collect(orders);
                } catch (Exception e) {
                    log.error("异常数据"+e,value);
                }

            }
        });

        //todo 测流Product清洗转换
        DataStream<String> ordersSource = source.getSideOutput(ProductsOutputTag);
        SingleOutputStreamOperator<Products> products = ordersSource.flatMap(new FlatMapFunction<String, Products>() {
            @Override
            public void flatMap(String value, Collector<Products> collector) throws Exception {
                try {
                String operType = JSONObject.parseObject(value).getString("operType");
                JSONObject jsonObject = JSON.parseObject(value);
                String bean = "";
                if (UPDATE.equals(operType)) {
                    bean = jsonObject.getString(AFTER);
                }
                if (INSERT.equals(operType)) {
                    bean = jsonObject.getString("columnInfo");
                }
                Products products = JSON.parseObject(bean, Products.class);
                collector.collect(products);
            } catch (Exception e) {

                log.error("异常数据"+e,value);
            }
        }});


        //todo Shipments清洗转换
        SingleOutputStreamOperator<Shipments> shipments = Shipments.flatMap(new FlatMapFunction<String, Shipments>() {
            @Override
            public void flatMap(String value, Collector<Shipments> collector) throws Exception {
                try {
                    String operType = JSONObject.parseObject(value).getString("operType");
                    JSONObject jsonObject = JSON.parseObject(value);
                    String bean = "";
                    if (UPDATE.equals(operType)) {
                        bean = jsonObject.getString(AFTER);
                    }
                    if (INSERT.equals(operType)) {
                        bean = jsonObject.getString("columnInfo");
                    }
                    Shipments shipments = JSON.parseObject(bean, Shipments.class);
                    collector.collect(shipments);
                } catch (Exception e) {

                    log.error("异常数据"+e,value);
                }
            }});



       // todo 三张表注册
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
         tableEnv.createTemporaryView("Products",products);
         tableEnv.createTemporaryView("Shipments",shipments);
         tableEnv.createTemporaryView("orders",orderStream);

        //todo 执行建表sql 连接starRocks,写出数据
        tableEnv.executeSql(" CREATE TABLE enriched_orders (\n" +
                "   order_id INT,\n" +
                "   order_date date,\n" +
                "   customer_name STRING,\n" +
                "   price DECIMAL,\n" +                              //(10, 5)
                "   product_id INT,\n" +
                "   order_status BOOLEAN,\n" +
                "   product_name STRING,\n" +
                "   product_description STRING,\n" +
                "   shipment_id STRING,\n" +
                "   origin STRING,\n" +
                "   destination STRING,\n" +
                "   is_arrived BOOLEAN,\n" +
                "   PRIMARY KEY (order_id,order_date) NOT ENFORCED" +
                " ) WITH (  'connector' = 'starrocks', \n" +
                "                'jdbc-url'='jdbc:mysql://192.168.24.98:9030,192.168.24.97:9030,192.168.24.99:9030', \n" +
                "                'load-url'='192.168.24.97:8030;192.168.24.98:8030;192.168.24.99:8030', \n" +
                "                'database-name' = 'khxx_data', \n" +
                "                'table-name' = 'enriched_ordersTest', \n" +
                "                'username' = 'khxx_data', \n" +
                "                'password' = 'khxx_data123456', \n" +
                "                'sink.buffer-flush.max-rows' = '65000', \n" +
                "                'sink.buffer-flush.max-bytes' = '300000000', \n" +
                "                'sink.buffer-flush.interval-ms' = '5000', \n" +
                "                'sink.properties.column_separator' = 'x01', \n" +
                "                'sink.properties.row_delimiter' = 'x02', \n" +
                "                'sink.max-retries' = '3'," +
                " 'sink.properties.max_filter_ratio' = '1')");


      tableEnv.executeSql("insert into enriched_orders " +
                "select o.orderId as order_id, o.orderDate as order_date ,o.customerName as customer_name ," +
                "o.price ,o.productId as product_id ,o.orderStatus as order_status ," +
                "p.name,p.description, s.shipmentId as shipment_id, s.origin, s.destination, s.Arrived as  is_arrived " +
                " from orders as o " +
                "left join  Products as p on p.id = o.orderId " +
                "LEFT JOIN Shipments as s ON o.orderId = s.orderId");

        Table table = tableEnv.sqlQuery("");
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table);

        env.execute(ThreeTableJoin.class.getSimpleName());

    }
}
