package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.IntervalProbe;
import com.hazelcast.util.Clock;

public class MaxLatencyProbe implements IntervalProbe<MaxLatencyResult, MaxLatencyProbe> {
    private long started;
    private long maxLatency;

    @Override
    public void started() {
        started = Clock.currentTimeMillis();
    }

    @Override
    public void startProbing(long time) {

    }

    @Override
    public void stopProbing(long time) {

    }

    @Override
    public void done() {
        long now = Clock.currentTimeMillis();
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
