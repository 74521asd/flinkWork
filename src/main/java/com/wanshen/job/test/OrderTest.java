package com.wanshen.job.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.wanshen.job.sink.OrderSink;
import com.wanshen.job.util.kafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import static com.wanshen.job.common.Config.*;
import static com.wanshen.job.common.MappingConfig.ORDERS_TABLE;

@Slf4j
public class OrderTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(kafkaUtil.getKafkaConsumer(KAFKA_TOPIC_SIT_TLSQL, "a12", DEV_KAFKA_IP));
              source.flatMap(new FlatMapFunction<String, JSONObject>() {
                  @Override
                  public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                      try {
                      JSONObject jsonObject = JSON.parseObject(value);

                      String bean="";
                      JSONObject b =null;

                      String operType = jsonObject.getString(OPENTYPE);
                      String tableName = jsonObject.getString("tableName");
                       if (ORDERS_TABLE.equals(tableName)){

                              if (UPDATE.equals(operType)) {
                                  bean = jsonObject.getString(AFTER);
                                  b = JSONObject.parseObject(bean);
                                  b.put("sql", "UPDATE Orders SET price = ?, customer_name = ?,product_id=?, order_status = ? where order_id = ?");

                              }
                              if (INSERT.equals(operType)) {
                                  bean = jsonObject.getString("columnInfo");
                                  b = JSONObject.parseObject(bean);
                                  b.put("sql", "insert into Orders (order_id,order_date,customer_name,price,product_id,order_status)values(?,?,?,?,?,?)");
                              }
                              if (DELETE.equals(operType)) {
                                  bean = jsonObject.getString(BEFORE);
                                  b = JSONObject.parseObject(bean);
                                  b.put("sql", "delete from Orders  where order_id =?");
                              }
                          }



                      System.out.println("执行"+ operType+"值"+ b);

                      b.put("bean","Orders");
                      b.put("MARK",operType);
                      out.collect(b);
                      } catch (Exception e) {
                          System.out.println("error " +value);
                      }
                  }
              }).addSink(new OrderSink());
              env.execute();
    }
}
