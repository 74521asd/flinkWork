package com.wanshen.job.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.etl.Process;
import com.wanshen.job.sink.ProductsSink;
import com.wanshen.job.util.kafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import static com.wanshen.job.common.Config.*;
import static com.wanshen.job.common.MappingConfig.PRODUCTS_TABLE;

@Slf4j
public class ProductsTest implements Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(kafkaUtil.getKafkaConsumer(KAFKA_TOPIC_SIT_TLSQL, "b1", DEV_KAFKA_IP));
        source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                System.out.println("ProductsValue = " + value);

                try {
                    JSONObject jsonObject = JSON.parseObject(value);

                    String bean="";
                    JSONObject b =null;

                    String operType = jsonObject.getString(OPENTYPE);
                    String tableName = jsonObject.getString("tableName");
                    if (PRODUCTS_TABLE.equals(tableName)){

                        if (UPDATE.equals(operType)) {
                            bean = jsonObject.getString(AFTER);
                            b = JSONObject.parseObject(bean);
                            b.put("sql", "UPDATE products SET  name = ?,description=? where id = ?");

                        }
                        if (INSERT.equals(operType)) {
                            bean = jsonObject.getString("columnInfo");
                            b = JSONObject.parseObject(bean);
                            b.put("sql", "insert into products (id,create_date,name,description)values(?,?,?,?)");
                        }
                        if (DELETE.equals(operType)) {
                            bean = jsonObject.getString(BEFORE);
                            b = JSONObject.parseObject(bean);
                            b.put("sql", "delete from products  where id =?");
                        }
                    }



                    System.out.println("执行"+ operType+"值"+ b);

                    b.put("bean","Products");
                    b.put("MARK",operType);
                    out.collect(b);
                } catch (Exception e) {
                    System.out.println("error= "+e+"=" +value);
                }
            }
        }).addSink(new ProductsSink());

        env.execute();

    }
    @Override
    public void work(StreamExecutionEnvironment env) {


    }
}
