package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.SimpleProbe;

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
        long stopOrNow = (stopped == 0 ? System.currentTimeMillis() : stopped);
        long durationMs = stopOrNow - started;
        return new OperationsPerSecondResult(((double) noOfOperations / durationMs) * 1000);
    }

    @Override
    public OperationsPerSecProbe createNew(Long arg) {
        return new OperationsPerSecProbe();
    }
}
