package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;

import java.util.concurrent.TimeUnit;

public class MaxLatencyProbe implements IntervalProbe<MaxLatencyResult, MaxLatencyProbe> {

    private long started;
    private long maxLatency;

    @Override
    public void started() {
        started = System.nanoTime();
    }

    @Override
    public void startProbing(long time) {
    }

    @Override
    public void stopProbing(long time) {
    }

    @Override
    public void done() {
        long latency = System.nanoTime() - started;
        maxLatency = Math.max(maxLatency, latency);
    }

    @Override
    public MaxLatencyResult getResult() {
        return new MaxLatencyResult(TimeUnit.NANOSECONDS.toMillis(maxLatency));
    }

    @Override
    public MaxLatencyProbe createNew(Long arg) {
        return new MaxLatencyProbe();
    }
}
