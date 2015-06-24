/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.ProbesType;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.SimpleProbe;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentProbe<R extends Result<R>, T extends IntervalProbe<R, T>> implements IntervalProbe<R, T> {

    private final transient ThreadLocal<T> threadLocalProbe = new ThreadLocal<T>();
    private final transient ConcurrentHashMap<Long, T> probeMap = new ConcurrentHashMap<Long, T>();
    private final transient ProbesType probesType;

    private transient long startedAt;

    public ConcurrentProbe(ProbesType probesType) {
        this.probesType = probesType;
    }

    @Override
    public void started() {
        getProbe().started();
    }

    @Override
    public void recordValue(long latencyNanos) {
        getProbe().recordValue(latencyNanos);
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
    public void setValues(long durationMs, int invocations) {
        getProbe().setValues(durationMs, invocations);
    }

    @Override
    public R getResult() {
        Iterator<T> probeIterator = probeMap.values().iterator();

        T firstProbe = findNextEnabledProbeOrNull(probeIterator);
        if (firstProbe == null) {
            return null;
        }

        R result = firstProbe.getResult();
        while (probeIterator.hasNext()) {
            T nextProbe = probeIterator.next();
            if (nextProbe.isDisabled()) {
                continue;
            }
            R nextData = nextProbe.getResult();
            result = result.combine(nextData);
        }

        return result;
    }

    private T findNextEnabledProbeOrNull(Iterator<T> probeIterator) {
        while (probeIterator.hasNext()) {
            T nextProbe = probeIterator.next();
            if (!nextProbe.isDisabled()) {
                return nextProbe;
            }
        }
        return null;
    }

    @Override
    public void disable() {
        getProbe().disable();
    }

    @Override
    public boolean isDisabled() {
        for (SimpleProbe probe : probeMap.values()) {
            if (!probe.isDisabled()) {
                return false;
            }
        }
        return true;
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

    int probeMapSize() {
        return probeMap.size();
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        throw new NotSerializableException();
    }

    private void readObject(ObjectInputStream in) throws IOException {
        throw new NotSerializableException();
    }
}
