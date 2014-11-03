package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.IntervalProbe;

public class DisabledProbe implements IntervalProbe<DisabledResult, DisabledProbe> {
    private static DisabledResult RESULT = new DisabledResult();
    public static final DisabledProbe INSTANCE = new DisabledProbe();

    private DisabledProbe() { }

    @Override
    public void started() {  }

    @Override
    public void startProbing(long time) {    }

    @Override
    public void stopProbing(long time) {   }

    @Override
    public void done() {   }

    @Override
    public DisabledResult getResult() {
        return RESULT;
    }

    @Override
    public DisabledProbe createNew(Long arg) {
        return this;
    }
}
