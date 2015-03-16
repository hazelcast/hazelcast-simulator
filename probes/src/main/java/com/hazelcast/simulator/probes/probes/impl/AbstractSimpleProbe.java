package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.Result;

public abstract class AbstractSimpleProbe<R extends Result<R>, T extends IntervalProbe<R, T>> implements IntervalProbe<R, T> {

    protected long started;
    protected long stopped;
    protected int invocations;

    @Override
    public void started() {
    }

    @Override
    public long getInvocationCount() {
        return invocations;
    }

    @Override
    public void startProbing(long timeStamp) {
        started = timeStamp;
    }

    @Override
    public void stopProbing(long timeStamp) {
        stopped = timeStamp;
    }

    @Override
    public R getResult() {
        if (started == 0) {
            throw new IllegalStateException("Can't get result as probe has no be started yet.");
        }
        long stopOrNow = (stopped == 0 ? System.currentTimeMillis() : stopped);
        long durationMs = stopOrNow - started;

        return getResult(durationMs);
    }

    protected abstract R getResult(long durationMs);
}
