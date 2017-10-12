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

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.worker.testcontainer.PropertyBinding.toPropertyName;
import static java.lang.System.nanoTime;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * The ConstantCombinedRateMetronome tries to make sure that requests are executed at a constant rate.
 * <p>
 * Normally with the {@link BusySpinningMetronome} or {@link SleepingMetronome} when 1 thread is blocked, it will not pick
 * up requests it should have picked up and it builds up a bubble. Once the thread is unblocked, this bubble needs to get
 * processed. Even though coordinated omission is resolved by determining the latency on the expected start time, while this
 * bubble is building up there is less load, and once the thread completes, the whole bubble needs to get processed.
 * <p>
 * With the ConstantCombinedRateMetronome this bubble is less likely to happen because as long as there is a thread available,
 * it will take over the work of the blocked threads. This way you get less bubbles and a more stable rate of requests.
 */
public class ConstantCombinedRateMetronome implements Metronome {

    private final long intervalNanos;
    private final boolean accountForCoordinatedOmission;
    private final AtomicLong nextExpectedStartNanos;

    ConstantCombinedRateMetronome(long intervalNanos, boolean accountForCoordinatedOmission) {
        this.intervalNanos = intervalNanos;
        this.accountForCoordinatedOmission = accountForCoordinatedOmission;
        this.nextExpectedStartNanos = new AtomicLong(nanoTime());
    }

    public ConstantCombinedRateMetronome(long intervalNanos, int threadCount, PropertyBinding binding, String prefix) {
        this(intervalNanos, binding.loadAsBoolean(toPropertyName(prefix, "accountForCoordinatedOmission"), true));
    }

    public ConstantCombinedRateMetronome(Metronome m) {
        ConstantCombinedRateMetronome master = (ConstantCombinedRateMetronome) m;
        this.intervalNanos = master.intervalNanos;
        this.accountForCoordinatedOmission = master.accountForCoordinatedOmission;
        this.nextExpectedStartNanos = master.nextExpectedStartNanos;
    }

    @Override
    public long waitForNext() {
        long expectedStartNanos;
        for (; ; ) {
            expectedStartNanos = nextExpectedStartNanos.get();
            long now;
            while ((now = nanoTime()) < expectedStartNanos) {
                // we can't pick up the request yet since it is too early.
                parkNanos(expectedStartNanos - now);
            }

            // if we manage to cas the item, we can execute the request, otherwise continue waiting.
            if (nextExpectedStartNanos.compareAndSet(expectedStartNanos, expectedStartNanos + intervalNanos)) {
                break;
            }
        }

        return accountForCoordinatedOmission ? expectedStartNanos : nanoTime();
    }

    public long getIntervalNanos() {
        return intervalNanos;
    }
}
