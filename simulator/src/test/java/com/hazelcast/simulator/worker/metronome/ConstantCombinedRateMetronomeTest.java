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

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ConstantCombinedRateMetronomeTest {

    @Test
    public void test() {
        long intervalNanos = TimeUnit.MILLISECONDS.toNanos(100);
        ConstantCombinedRateMetronome master = new ConstantCombinedRateMetronome(intervalNanos, true);

        ConstantCombinedRateMetronome metronome1 = new ConstantCombinedRateMetronome(master);
        ConstantCombinedRateMetronome metronome2 = new ConstantCombinedRateMetronome(master);

        long next = metronome1.waitForNext() + intervalNanos;

        assertEquals(next, metronome1.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome1.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome2.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome2.waitForNext());
        next += intervalNanos;
        assertEquals(next, metronome1.waitForNext());
    }
}
