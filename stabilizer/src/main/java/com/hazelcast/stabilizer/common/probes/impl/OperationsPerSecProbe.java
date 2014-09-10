package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.SimpleProbe;
import com.hazelcast.util.Clock;

public class OperationsPerSecProbe implements SimpleProbe<OperationsPerSecondResult, OperationsPerSecProbe> {
    private long noOfOperations;
    private long started;
    private long stopped;

    @Override
    public void startProbing(long time) {
        started = time;
    }

    @Override
    public void stopProbing(long time) {
        stopped = time;
    }

    @Override
    public void done() {
        noOfOperations++;
    }

    @Override
    public OperationsPerSecondResult getResult() {
        if (started == 0) {
            throw new IllegalStateException("Can't get result as probe has no be started yet.");
        }
        long stopOrNow = (stopped == 0 ? Clock.currentTimeMillis() : stopped);
        long durationMs = stopOrNow - started;
        return new OperationsPerSecondResult(((double)noOfOperations / durationMs) * 1000);
    }

    @Override
    public OperationsPerSecProbe createNew(Long arg) {
        return new OperationsPerSecProbe();
    }
}
