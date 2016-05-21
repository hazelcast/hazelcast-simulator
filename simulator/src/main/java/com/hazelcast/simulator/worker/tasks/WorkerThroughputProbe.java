/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.probes.Probe;
import org.HdrHistogram.Histogram;

import java.util.Set;

public class WorkerThroughputProbe implements Probe {

    private final Set<IWorker> workers;

    public WorkerThroughputProbe(Set<IWorker> workers) {
        this.workers = workers;
    }

    @Override
    public boolean isMeasuringLatency() {
        return false;
    }

    @Override
    public boolean isPartOfTotalThroughput() {
        return true;
    }

    @Override
    public void done(long started) {
    }

    @Override
    public void recordValue(long latencyNanos) {
    }

    @Override
    public void inc(long count) {

    }

    @Override
    public Histogram getIntervalHistogram() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long get() {
        long result = 0;
        for (IWorker worker : workers) {
            result += worker.getIteration();
        }
        return result;
    }
}
