package com.hazelcast.stabilizer.common.probes;

public interface IntervalProbe<R extends Result<R>, T extends SimpleProbe<R, T>> extends SimpleProbe<R, T>{
    public void started();
}
