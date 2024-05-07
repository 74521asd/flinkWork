package com.wanshen.job.func;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;

/**
 * flink  异步io 采用非阻塞异步方式并行写入数据，极大提高吞吐能力
 * 注意：如果针对传统数据库，如MySQL，Oracle这种数据库，谨慎使用，容易写崩数据库
 * @param <T>
 */
public class AsyncIO<T> extends RichAsyncFunction<T,T> {


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        resultFuture.complete(Collections.singletonList(input));

    }
}
