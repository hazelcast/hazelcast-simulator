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
package com.hazelcast.simulator.worker.metronome;

import static java.lang.System.nanoTime;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.apache.commons.lang3.RandomUtils.nextLong;

/**
 * Simple {@link Metronome} implementation which sleeps on a fixed interval.
 * <p>
 * The wait interval on the first {@link #waitForNext()} call is randomized.
 */
public final class SleepingMetronome implements Metronome {

    private final long intervalNanos;
    private final boolean accountForCoordinatedOmission;
    private long nextNanos;

    public SleepingMetronome(long intervalNanos, boolean accountForCoordinatedOmission) {
        this.intervalNanos = intervalNanos;
        this.accountForCoordinatedOmission = accountForCoordinatedOmission;
    }

    @Override
    public long waitForNext() {
        if (nextNanos == 0) {
            nextNanos = nanoTime() + nextLong(0, intervalNanos);
        }

        long delayNanos = nextNanos - nanoTime();
        if (delayNanos > 0) {
            parkNanos(delayNanos);
        }

        long expectedStartNanos = nextNanos;
        nextNanos += intervalNanos;
        return accountForCoordinatedOmission ? expectedStartNanos : nanoTime();
    }

    public long getIntervalNanos() {
        return intervalNanos;
    }
}
