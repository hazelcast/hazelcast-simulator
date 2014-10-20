package com.hazelcast.stabilizer.probes.probes;

public interface IntervalProbe<R extends Result<R>, T extends SimpleProbe<R, T>> extends SimpleProbe<R, T>{
    public void started();
}
