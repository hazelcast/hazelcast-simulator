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

import static org.apache.commons.lang3.RandomUtils.nextLong;

/**
 * Simple {@link Metronome} implementation which busy loops on a fixed interval.
 *
 * The wait interval on the first {@link #waitForNext()} call is randomized.
 *
 * It is recommended to create a new instance for each worker thread, so they are clocked interleaved.
 */
final class BusySpinningMetronome implements Metronome {

    private final long intervalNanos;

    private long waitUntil;

    BusySpinningMetronome(long intervalNanos) {
        this.intervalNanos = intervalNanos;
    }

    @Override
    public void waitForNext() {
        // set random interval on the first run
        if (waitUntil == 0) {
            waitUntil = System.nanoTime() + nextLong(0, intervalNanos);
        }

        // busy loop
        long now;
        do {
            now = System.nanoTime();
        } while (now < waitUntil);

        // set regular interval for next call
        waitUntil = now + intervalNanos;
    }
}
