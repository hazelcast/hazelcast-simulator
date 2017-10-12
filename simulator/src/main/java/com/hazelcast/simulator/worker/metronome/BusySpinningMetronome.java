/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker.metronome;

import com.hazelcast.simulator.worker.testcontainer.PropertyBinding;

import static com.hazelcast.simulator.worker.testcontainer.PropertyBinding.toPropertyName;
import static java.lang.System.nanoTime;
import static org.apache.commons.lang3.RandomUtils.nextLong;

/**
 * Simple {@link Metronome} implementation which busy loops on a fixed interval.
 *
 * If an execution takes more than the intervalNanos, the request are 'queued' and get processed as soon as the system
 * has time for time. This queue will get processed as fast as possible and there metronome will not introduce any deliberate
 * slowdowns. For more information see:
 * https://vanilla-java.github.io/2016/07/20/Latency-for-a-set-Throughput.html
 *
 * This metronome is not suited if there are just a one or a few test threads due to the busy spinning.
 *
 * The wait interval on the first {@link #waitForNext()} call is randomized.
 */
public final class BusySpinningMetronome implements Metronome {

    private final long intervalNanos;
    private final boolean accountForCoordinatedOmission;
    private long nextNanos;

    BusySpinningMetronome(long intervalNanos, boolean accountForCoordinatedOmission) {
        this.intervalNanos = intervalNanos;
        this.accountForCoordinatedOmission = accountForCoordinatedOmission;
    }

    public BusySpinningMetronome(long intervalNanos, int threadCount, PropertyBinding binding, String prefix) {
        this(intervalNanos * threadCount, binding.loadAsBoolean(toPropertyName(prefix, "accountForCoordinatedOmission"), true));
    }

    public BusySpinningMetronome(Metronome m) {
        BusySpinningMetronome master = (BusySpinningMetronome) m;
        this.intervalNanos = master.intervalNanos;
        this.accountForCoordinatedOmission = master.accountForCoordinatedOmission;
    }

    @Override
    public long waitForNext() {
        // set random interval on the first run
        if (nextNanos == 0) {
            nextNanos = nanoTime() + nextLong(0, intervalNanos);
        }

        long now;
        do {
            now = nanoTime();
        } while (now < nextNanos);

        long expectedStartNanos = nextNanos;
        nextNanos = expectedStartNanos + intervalNanos;
        return accountForCoordinatedOmission ? expectedStartNanos : nanoTime();
    }

    public long getIntervalNanos() {
        return intervalNanos;
    }
}
