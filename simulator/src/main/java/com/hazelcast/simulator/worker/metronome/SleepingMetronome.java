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

import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.simulator.worker.testcontainer.PropertyBinding.toPropertyName;
import static java.lang.System.nanoTime;
import static org.apache.commons.lang3.RandomUtils.nextLong;

/**
 * Simple {@link Metronome} implementation which sleeps on a fixed interval. The SleepingMetronome is best when there
 * are many threads and you don't want all threads to spin.
 * <p>
 * The wait interval on the first {@link #waitForNext()} call is randomized.
 */
public final class SleepingMetronome implements Metronome {

    private final long intervalNanos;
    private final boolean accountForCoordinatedOmission;
    private long nextNanos;

    SleepingMetronome(long intervalNanos, boolean accountForCoordinatedOmission) {
        this.intervalNanos = intervalNanos;
        this.accountForCoordinatedOmission = accountForCoordinatedOmission;
    }

    public SleepingMetronome(long intervalNanos, int threadCount, PropertyBinding binding, String prefix) {
        this(intervalNanos * threadCount, binding.loadAsBoolean(toPropertyName(prefix, "accountForCoordinatedOmission"), true));
    }

    public SleepingMetronome(Metronome m) {
        SleepingMetronome master = (SleepingMetronome) m;
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
        while ((now = System.nanoTime()) < nextNanos) {
            LockSupport.parkNanos(nextNanos - now);
        }

        long expectedStartNanos = nextNanos;
        nextNanos += intervalNanos;
        return accountForCoordinatedOmission ? expectedStartNanos : nanoTime();
    }

    public long getIntervalNanos() {
        return intervalNanos;
    }
}
