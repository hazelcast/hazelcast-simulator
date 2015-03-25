package com.hazelcast.simulator.probes.probes;

public interface SimpleProbe<R extends Result<R>, T extends SimpleProbe<R, T>> {

    void done();

    long getInvocationCount();

    void startProbing(long timeStamp);

    void stopProbing(long timeStamp);

    R getResult();
}
