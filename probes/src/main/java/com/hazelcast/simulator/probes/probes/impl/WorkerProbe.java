package com.hazelcast.simulator.probes.probes.impl;

/**
 * This probe will measure the throughput but won't save its result.
 * <p/>
 * It can be used to provide the information for performance measuring during a test without polluting the worker
 * directory with a result file.
 */
public class WorkerProbe extends AbstractIntervalProbe<DisabledResult, WorkerProbe> {

    @Override
    public void done() {
        invocations++;
    }

    @Override
    public DisabledResult getResult() {
        return new DisabledResult();
    }
}
