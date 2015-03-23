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
    public void startProbing(long time) {
    }

    @Override
    public void stopProbing(long time) {
    }

    @Override
    public void done() {
    }

    @Override
    public DisabledResult getResult() {
        return RESULT;
    }

    @Override
    public DisabledProbe createNew(Long arg) {
        return this;
    }
}
