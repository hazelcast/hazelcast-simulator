package com.hazelcast.stabilizer.common.probes;

import com.hazelcast.util.ConstructorFunction;

public interface SimpleProbe<R extends Result<R>, T extends SimpleProbe<R, T>> extends ConstructorFunction<Long, T> {
    void startProbing(long time);
    void stopProbing(long time);

    void done();
    R getResult();
}
