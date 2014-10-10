package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.IntervalProbe;

public class MaxLatencyProbe implements IntervalProbe<MaxLatencyResult, MaxLatencyProbe> {
    private long started;
    private long maxLatency;

    @Override
    public void started() {
        started = System.currentTimeMillis();
    }

    @Override
    public void startProbing(long time) {

    }

    @Override
    public void stopProbing(long time) {

    }

    @Override
    public void done() {
        long now = System.currentTimeMillis();
        long latency = now - started;
        maxLatency = Math.max(maxLatency, latency);
    }

    @Override
    public MaxLatencyResult getResult() {
        return new MaxLatencyResult(maxLatency);
    }

    @Override
    public MaxLatencyProbe createNew(Long arg) {
        return new MaxLatencyProbe();
    }
}
