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

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static org.apache.commons.lang3.RandomUtils.nextLong;

/**
 * Simple {@link Metronome} implementation which sleeps on a fixed interval.
 *
 * The wait interval on the first {@link #waitForNext()} call is randomized.
 *
 * It is recommended to create a new instance for each worker thread, so they are clocked interleaved.
 */
final class SleepingMetronome implements Metronome {

    private final long intervalNanos;

    private boolean isFirstSleep = true;

    SleepingMetronome(long intervalNanos) {
        this.intervalNanos = intervalNanos;
    }

    @Override
    public void waitForNext() {
        // sleep random interval on the first run
        if (isFirstSleep) {
            sleepNanos(nextLong(0, intervalNanos));
            isFirstSleep = false;
            return;
        }

        sleepNanos(intervalNanos);
    }
}
