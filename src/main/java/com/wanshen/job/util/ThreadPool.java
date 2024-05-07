package com.wanshen.job.util;

import org.codehaus.commons.nullanalysis.NotNull;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 任俊龙
 */
public class ThreadPool {

    private static final int cores = Runtime.getRuntime().availableProcessors();
    private static final int limitSize = 1000000;

    private static final ReentrantLock reentrantLock = new ReentrantLock();
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(Math.round(cores>>>1),
            cores,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(limitSize),
            new ThreadFactory() {
                final AtomicInteger a = new AtomicInteger(0);
                @Override
                public Thread newThread(@NotNull Runnable r) {
                    return new Thread(r,"test"+a.getAndAdd(1));
                }
            },new ThreadPoolExecutor.AbortPolicy());


    public static ThreadPoolExecutor getExecutor(){
      return  executor;
    }

    public static void main(String[] args) {


    }
}
