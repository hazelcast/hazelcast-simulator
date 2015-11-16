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
package com.hazelcast.simulator.worker.metronome;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.round;
import static org.apache.commons.lang3.RandomUtils.nextLong;

/**
 * Simple {@link Metronome} implementation which busy loops on a fixed interval.
 *
 * The wait interval on the first {@link #waitForNext()} call is randomized.
 *
 * It is recommended to create a new instance for each worker thread, so they are clocked interleaved.
 */
public final class SimpleMetronome implements Metronome {

    private static final Metronome EMPTY_METRONOME = new EmptyMetronome();

    private final long intervalNanos;

    private long waitUntil;

    private SimpleMetronome(long intervalNanos) {
        this.intervalNanos = intervalNanos;
    }

    /**
     * Creates a {@link Metronome} instance with a fixed millisecond interval.
     *
     * @param intervalMs wait interval in milliseconds
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedIntervalMs(int intervalMs) {
        if (intervalMs == 0) {
            return EMPTY_METRONOME;
        }
        return new SimpleMetronome(TimeUnit.MILLISECONDS.toNanos(intervalMs));
    }

    /**
     * Creates a {@link Metronome} instance with a fixed frequency in Hz.
     *
     * If the frequency is 0 Hz the method {@link #waitForNext()} will have no delay.
     *
     * @param frequency frequency
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedFrequency(float frequency) {
        if (frequency == 0) {
            return EMPTY_METRONOME;
        }

        long intervalNanos = round((double) TimeUnit.SECONDS.toNanos(1) / frequency);
        return new SimpleMetronome(intervalNanos);
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

    private static class EmptyMetronome implements Metronome {

        @Override
        public void waitForNext() {
        }
    }
}
