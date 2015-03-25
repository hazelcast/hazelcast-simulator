package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;

public final class DisabledProbe implements IntervalProbe<DisabledResult, DisabledProbe> {

    public static final DisabledProbe INSTANCE = new DisabledProbe();

    private static final DisabledResult RESULT = new DisabledResult();

    private DisabledProbe() {
    }

    @Override
    public void started() {
    }

    @Override
    public void done() {
    }

    @Override
    public long getInvocationCount() {
        return 0;
    }

    @Override
    public void startProbing(long timeStamp) {
    }

    @Override
    public void stopProbing(long timeStamp) {
    }

    @Override
    public DisabledResult getResult() {
        return RESULT;
    }
}
