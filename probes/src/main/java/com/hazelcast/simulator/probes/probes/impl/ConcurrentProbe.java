package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.ProbesType;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.SimpleProbe;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentProbe<R extends Result<R>, T extends IntervalProbe<R, T>> implements IntervalProbe<R, T> {

    private final ThreadLocal<T> threadLocalProbe = new ThreadLocal<T>();
    private final ConcurrentHashMap<Long, T> probeMap = new ConcurrentHashMap<Long, T>();
    private final ProbesType probesType;

    private long startedAt;

    public ConcurrentProbe(ProbesType probesType) {
        this.probesType = probesType;
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
    public long getInvocationCount() {
        long invocations = 0;
        for (T probe : probeMap.values()) {
            invocations += probe.getInvocationCount();
        }

        return invocations;
    }

    @Override
    public void startProbing(long timeStamp) {
        startedAt = timeStamp;
    }

    @Override
    public void stopProbing(long timeStamp) {
        for (SimpleProbe probe : probeMap.values()) {
            probe.stopProbing(timeStamp);
        }
    }

    @Override
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
        if (probe != null) {
            return probe;
        }

        long id = Thread.currentThread().getId();
        try {
            probe = probesType.createInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        probeMap.put(id, probe);
        probe.startProbing(startedAt);
        threadLocalProbe.set(probe);

        return probe;
    }
}
