package com.wanshen.job.test;

import com.wanshen.job.util.ThreadPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CompletableFutureTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CompletableFuture<Void> future = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                String o = null;
                return o;
            }
        }, ThreadPool.getExecutor()).thenAcceptAsync(new Consumer<String>() {
            @Override
            public void accept(String o) {


            }
        });
        future.whenComplete(new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void unused, Throwable throwable) {

            }
        });
        future.get();

    }
}
