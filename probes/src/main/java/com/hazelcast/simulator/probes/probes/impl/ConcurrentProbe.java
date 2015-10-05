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

import com.hazelcast.simulator.probes.probes.Probe;
import com.hazelcast.simulator.probes.probes.Result;
import org.HdrHistogram.Histogram;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentProbe implements Probe {

    private final ThreadLocal<Probe> threadLocalProbe = new ThreadLocal<Probe>();
    private final ConcurrentHashMap<Long, Probe> probeMap = new ConcurrentHashMap<Long, Probe>();

    private long startedAt;

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
        for (Probe probe : probeMap.values()) {
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
        for (Probe probe : probeMap.values()) {
            probe.stopProbing(timeStamp);
        }
    }

    @Override
    public void setValues(long durationMs, long invocations) {
        getProbe().setValues(durationMs, invocations);
    }

    @Override
    public Histogram getIntervalHistogram() {
        Iterator<Probe> probeIterator = probeMap.values().iterator();

        Probe firstProbe = findNextEnabledProbeOrNull(probeIterator);
        if (firstProbe == null) {
            return null;
        }

        Histogram intervalHistogram = firstProbe.getIntervalHistogram().copy();
        while (probeIterator.hasNext()) {
            Probe nextProbe = probeIterator.next();
            if (nextProbe.isDisabled()) {
                continue;
            }
            Histogram nextHistogram = nextProbe.getIntervalHistogram();
            intervalHistogram.add(nextHistogram);
        }

        return intervalHistogram;
    }

    @Override
    public Result getResult() {
        Iterator<Probe> probeIterator = probeMap.values().iterator();

        Probe firstProbe = findNextEnabledProbeOrNull(probeIterator);
        if (firstProbe == null) {
            return null;
        }

        Result result = firstProbe.getResult();
        while (probeIterator.hasNext()) {
            Probe nextProbe = probeIterator.next();
            if (nextProbe.isDisabled()) {
                continue;
            }
            Result nextData = nextProbe.getResult();
            result = result.combine(nextData);
        }

        return result;
    }

    private Probe findNextEnabledProbeOrNull(Iterator<Probe> probeIterator) {
        while (probeIterator.hasNext()) {
            Probe nextProbe = probeIterator.next();
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
        for (Probe probe : probeMap.values()) {
            if (!probe.isDisabled()) {
                return false;
            }
        }
        return true;
    }

    Probe getProbe() {
        Probe probe = threadLocalProbe.get();
        if (probe != null) {
            return probe;
        }

        long id = Thread.currentThread().getId();
        probe = new ProbeImpl();
        probeMap.put(id, probe);
        probe.startProbing(startedAt);
        threadLocalProbe.set(probe);

        return probe;
    }

    int probeMapSize() {
        return probeMap.size();
    }
}
