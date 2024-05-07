package com.wanshen.job.common;

public class Config {
    public static final String  TEST_KAFKA_IP  = "127.0.0.1:8848";
    public static final String  DEV_KAFKA_IP  = "192.168.24.92:9092,192.168.24.95:9092,192.168.24.96:9092";
    public static final String  DEV_KAFKA_1  = "192.168.24.92:9092";
    public static final String  HDFS_IP  = "hdfs://hadoop001:8020/sds";
    public static final String  KAFKA_TOPIC_DEV  = "test";
    public static final String  KAFKA_TOPIC_SIT_ORACLE  = "crmii-dev-all-dsg";
    public static final String  KAFKA_TOPIC_SIT_TLSQL  = "tdsql241-dev-all-dsg";
    public static final String  KAFKA_GROUP_ID_DEV  = "dev_ren12";
    public static final String  OFFSET_RESET = "earliest";
    public static final String  OFFSET_RESET1 = "latest";
    public static final String  INSERT = "I";
    public static final String  UPDATE = "U";
    public static final String  DELETE = "D";
    public static final String  BEFORE = "Before";
    public static final String  AFTER = "After";
    public static final String  OPENTYPE = "operType";
}
