package com.wanshen.job.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.wanshen.job.common.Config.OFFSET_RESET;

/**
 * flink kafka消费端工具类
 */

public class kafkaUtil {
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId,String kafkaIp){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaIp);
        prop.setProperty("group.id",groupId);
        prop.setProperty("auto.offset.reset",OFFSET_RESET);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

                if (consumerRecord==null || consumerRecord.value()==null) return null;
                String s = new String(consumerRecord.value());
                s.getBytes(StandardCharsets.UTF_8);
                return s;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        },prop);



    }

}
