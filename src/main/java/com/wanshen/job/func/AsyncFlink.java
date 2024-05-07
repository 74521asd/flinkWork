package com.wanshen.job.func;

import com.wanshen.job.util.ThreadPool;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;


import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * flink  异步io 采用非阻塞异步方式并行写入数据，极大提高吞吐能力
 * 注意：如果针对传统数据库，如MySQL，Oracle这种数据库，谨慎使用，容易写崩数据库
 * @param <T>
 * @author 任俊龙
 *
 */
public class AsyncFlink<T> extends RichAsyncFunction<T,T> {
    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        CompletableFuture<Void> future = CompletableFuture.supplyAsync(new Supplier<T>() {
            @Override
            public T get() {
                //此处t是一个bean对象，在此处可添加对此对象的加工逻辑

              return t;
            }
        }, ThreadPool.getExecutor())
                .exceptionally(e-> null)
                .handle((h,e)->h)//暂时不做逻辑处理
                .thenAcceptAsync(new Consumer<T>() {
            @Override
            public void accept(T o) {
                resultFuture.complete(Collections.singletonList(o));
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }
}
