package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.Result;
import com.hazelcast.stabilizer.common.probes.SimpleProbe;
import com.hazelcast.util.ConstructorFunction;

public class ConcurrentSimpleProbe<R extends Result<R>, T extends SimpleProbe<R, T>>
        extends AbstractConcurrentProbe<R, T> implements SimpleProbe<R, ConcurrentSimpleProbe<R, T>> {

    private final ConstructorFunction<Long, T> constructorFunction;

    public ConcurrentSimpleProbe(ConstructorFunction<Long, T> constructorFunction) {
        super(constructorFunction);
        this.constructorFunction = constructorFunction;
    }

    @Override
    public void done() {
        getProbe().done();
    }

    @Override
    public ConcurrentSimpleProbe<R, T> createNew(Long arg) {
        return new ConcurrentSimpleProbe<R, T>(constructorFunction);
    }

}
