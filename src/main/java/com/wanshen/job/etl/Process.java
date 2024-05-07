package com.wanshen.job.etl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *ETL作业类接口
 * @param <T>  入参类型
 * @param <U>  出参类型
 */
public interface Process<T,U>{
   public  void work(StreamExecutionEnvironment env) ;


}
