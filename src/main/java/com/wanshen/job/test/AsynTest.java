package com.wanshen.job.test;


import com.wanshen.job.util.ThreadPool;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class AsynTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(5);
        Future<String> submit = pool.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                String o = null;
                return o;
            }
        });
        String s = submit.get();
        CompletableFuture<String> future = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                String o = null;
                return o;
            }
        }, ThreadPool.getExecutor());
        ArrayList<Throwable> throwables = new ArrayList<>();
        future.whenComplete(new BiConsumer<String, Throwable>() {
            @Override
            public void accept(String s, Throwable throwable) {
                throwables.add(throwable);

            }
        });


        String s1 = future.get();


    }
}
