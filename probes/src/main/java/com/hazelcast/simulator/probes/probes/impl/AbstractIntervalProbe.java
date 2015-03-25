package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.Result;

public abstract class AbstractIntervalProbe<R extends Result<R>, T extends IntervalProbe<R, T>> implements IntervalProbe<R, T> {

    protected long started;
    protected int invocations;

    @Override
    public void started() {
        started = System.nanoTime();
    }

    @Override
    public long getInvocationCount() {
        return invocations;
    }

    @Override
    public void startProbing(long timeStamp) {
    }

    @Override
    public void stopProbing(long timeStamp) {
    }
}
