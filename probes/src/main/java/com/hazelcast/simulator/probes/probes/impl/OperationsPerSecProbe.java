package com.hazelcast.simulator.probes.probes.impl;

public class OperationsPerSecProbe extends AbstractSimpleProbe<OperationsPerSecResult, OperationsPerSecProbe> {

    @Override
    public void done() {
        invocations++;
    }

    @Override
    public OperationsPerSecResult getResult(long durationMs) {
        return new OperationsPerSecResult(invocations, ((double) invocations / durationMs) * 1000);
    }
}
