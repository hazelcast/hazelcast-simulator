package com.hazelcast.simulator.probes.probes.impl;

import java.util.concurrent.TimeUnit;

public class MaxLatencyProbe extends AbstractIntervalProbe<MaxLatencyResult, MaxLatencyProbe> {

    private long maxLatency;

    @Override
    public void done() {
        if (started == 0) {
            throw new IllegalStateException("You have to call started() before done()");
        }
        long latency = System.nanoTime() - started;
        maxLatency = Math.max(maxLatency, latency);
        invocations++;
    }

    @Override
    public MaxLatencyResult getResult() {
        return new MaxLatencyResult(TimeUnit.NANOSECONDS.toMillis(maxLatency));
    }
}
