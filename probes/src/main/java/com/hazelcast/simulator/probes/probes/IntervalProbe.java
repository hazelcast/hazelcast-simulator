package com.hazelcast.simulator.probes.probes;

public interface IntervalProbe<R extends Result<R>, T extends SimpleProbe<R, T>> extends SimpleProbe<R, T> {

    void started();
}
