package com.wanshen.job.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.util.kafkaUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.wanshen.job.common.Config.*;
import static com.wanshen.job.common.Config.DEV_KAFKA_IP;

public class test3 {
    public static void main(String[] args) {
       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(kafkaUtil.getKafkaConsumer(KAFKA_TOPIC_SIT_TLSQL, "b39", DEV_KAFKA_IP));
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(t -> {
            JSONObject jsonObject = JSON.parseObject(t);
            Tuple2<String, Integer> tuple2 = new Tuple2<>(jsonObject.getString("dd"), jsonObject.getInteger("s"));
            return tuple2;
        });







    }
}
