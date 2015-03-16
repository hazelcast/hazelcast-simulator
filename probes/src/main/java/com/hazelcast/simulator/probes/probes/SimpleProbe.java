package com.hazelcast.simulator.probes.probes;


import com.hazelcast.simulator.probes.probes.util.ConstructorFunction;

public interface SimpleProbe<R extends Result<R>, T extends SimpleProbe<R, T>> extends ConstructorFunction<Long, T> {

    void startProbing(long time);

    void stopProbing(long time);

    void done();

    R getResult();
}
