package com.hazelcast.simulator.worker;

public interface Worker {

    void shutdown();

    boolean startPerformanceMonitor();

    void shutdownPerformanceMonitor();
}
