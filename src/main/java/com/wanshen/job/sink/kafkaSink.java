package com.wanshen.job.sink;


import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.commons.nullanalysis.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

import static com.wanshen.job.common.Config.KAFKA_TOPIC_DEV;
import static com.wanshen.job.common.Config.TEST_KAFKA_IP;


/**
 * 向Kafka内写数据
 * @author
 */
public class kafkaSink {
  public static void main(String[] args) throws Exception {
    //1.获取流的执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//    DataStreamSource<String> stringDataStream = env.readTextFile("C:\\Users\\任俊龙\\Desktop\\zhuge.txt");
//
//    SingleOutputStreamOperator<String> processStream = stringDataStream.process(new ProcessFunction<String, String>() {
//      @Override
//      public void processElement(String s, Context context, Collector<String> out) throws Exception {
//        try {
//          JSONObject jsonObject = JSON.parseObject(s);
//          if (jsonObject.getInteger("app_id") == 5)
//            out.collect(s);
//
//        } catch (Exception ignored) {
//
//        }
//      }
//    });
    ArrayList<String> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
String  json ="{\"Owner\":\"HR_JX\",\"tableName\":\"ORDERS\",\"operType\":\"U\",\"OP_TIME\":\"2023-06-12 14:48:58\",\"LOADERTIME\":\"2023-06-12 14:50:34\",\"SEQ_ID\":14,\"Before\":{\"order_id\":10006,\"order_date\":\"2020-07-30 12:01:30\",\"customer_name\":\"Edward\",\"price\":25.25000,\"product_id\":1066,\"order_status\":0},\"After\":{\"order_id\":10006,\"order_date\":\"2020-07-30 12:00:30\",\"customer_name\":\"Edward\",\"price\":25.25000,\"product_id\":1067,\"order_status\":0},\"PK\":{\"order_id\":\"10006\"}}";
       json=json.replaceAll("\\\\\"", "");
        System.out.println("s = " + json);
        list.add(json);
    }
    DataStreamSource<String> source = env.fromCollection(list);
    SingleOutputStreamOperator<String> map = source.map(t -> JSON.toJSONString(t));


//    FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("localhost:9092", "pay_zg_total ", new SimpleStringSchema());

//    processStream.addSink(kafkaProducer);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", TEST_KAFKA_IP);
    KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
      @Override
      public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        return new ProducerRecord<>(
                KAFKA_TOPIC_DEV, // target topic
            element.getBytes(StandardCharsets.UTF_8)); // record contents
      }
    };

    FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
            KAFKA_TOPIC_DEV,             // target topic
        serializationSchema,    // serialization schema
        properties,             // producer config
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

    map.addSink(myProducer);

    env.execute();
  }
}
