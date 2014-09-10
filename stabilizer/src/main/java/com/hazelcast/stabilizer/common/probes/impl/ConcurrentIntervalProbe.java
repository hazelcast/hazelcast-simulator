package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.IntervalProbe;
import com.hazelcast.stabilizer.common.probes.Result;
import com.hazelcast.util.ConstructorFunction;

public class ConcurrentIntervalProbe<R extends Result<R>, T extends IntervalProbe<R, T>>
        extends AbstractConcurrentProbe<R, T> implements IntervalProbe<R, ConcurrentIntervalProbe<R, T>> {

    private final ConstructorFunction<Long, T> constructorFunction;

    public ConcurrentIntervalProbe(ConstructorFunction<Long, T> constructorFunction) {
        super(constructorFunction);
        this.constructorFunction = constructorFunction;
    }

    @Override
    public void started() {
        getProbe().started();
    }

    @Override
    public void done() {
        getProbe().done();
    }

    @Override
    public ConcurrentIntervalProbe<R, T> createNew(Long arg) {
        return new ConcurrentIntervalProbe<R, T>(constructorFunction);
    }
}
