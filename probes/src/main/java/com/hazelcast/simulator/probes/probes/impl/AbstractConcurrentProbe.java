package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.probes.probes.util.ConcurrencyUtil;
import com.hazelcast.simulator.probes.probes.util.ConstructorFunction;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractConcurrentProbe<R extends Result<R>, T extends SimpleProbe<R, T>> {

    private final ThreadLocal<T> threadLocalProbe;
    private final ConcurrentHashMap<Long, T> probeMap;
    private final ConstructorFunction<Long, T> constructorFunction;

    private long startedAt;

    public AbstractConcurrentProbe(ConstructorFunction<Long, T> constructorFunction) {
        this.constructorFunction = constructorFunction;
        this.probeMap = new ConcurrentHashMap<Long, T>();
        this.threadLocalProbe = new ThreadLocal<T>();
    }

    public R getResult() {
        Iterator<T> probeIterator = probeMap.values().iterator();
        if (!probeIterator.hasNext()) {
            return null;
        }

        R result = probeIterator.next().getResult();
        while (probeIterator.hasNext()) {
            T nextProbe = probeIterator.next();
            R nextData = nextProbe.getResult();
            result = result.combine(nextData);
        }

        return result;
    }

    T getProbe() {
        T probe = threadLocalProbe.get();
        if (probe == null) {
            long id = Thread.currentThread().getId();
            probe = ConcurrencyUtil.getOrPutIfAbsent(probeMap, id, constructorFunction);
            probe.startProbing(startedAt);
            threadLocalProbe.set(probe);
        }

        return probe;
    }

    public void startProbing(long time) {
        startedAt = time;
    }

    public void stopProbing(long time) {
        for (T probe : probeMap.values()) {
            probe.stopProbing(time);
        }
    }
}
