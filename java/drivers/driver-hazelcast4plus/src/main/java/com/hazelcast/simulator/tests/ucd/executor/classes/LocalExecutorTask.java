package com.hazelcast.simulator.tests.ucd.executor.classes;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class LocalExecutorTask implements Callable, Serializable {
    private static final long serialVersionUID = 8301151618785236415L;
    private final long startTime;

    public LocalExecutorTask(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public Object call() {
        return startTime;
    }
}
