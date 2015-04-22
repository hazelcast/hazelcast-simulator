package com.hazelcast.simulator.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class ExecutorFactory {

    public static ExecutorService createFixedThreadPool(int poolSize, Class classType) {
        return createFixedThreadPool(poolSize, getName(classType));
    }

    public static ExecutorService createFixedThreadPool(int poolSize, String namePrefix) {
        return Executors.newFixedThreadPool(poolSize, createThreadFactory(namePrefix));
    }

    public static ExecutorService createCachedThreadPool(Class classType) {
        return createCachedThreadPool(getName(classType));
    }

    public static ExecutorService createCachedThreadPool(String namePrefix) {
        return Executors.newCachedThreadPool(createThreadFactory(namePrefix));
    }

    public static ScheduledExecutorService createScheduledThreadPool(int poolSize, Class classType) {
        return createScheduledThreadPool(poolSize, getName(classType));
    }

    public static ScheduledExecutorService createScheduledThreadPool(int poolSize, String namePrefix) {
        return Executors.newScheduledThreadPool(poolSize, createThreadFactory(namePrefix));
    }

    private static String getName(Class classType) {
        return classType.getSimpleName().toLowerCase();
    }

    private static ThreadFactory createThreadFactory(String namePrefix) {
        return new ThreadFactoryBuilder().setNameFormat(namePrefix + "-thread-%d").build();
    }
}
