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

import static com.hazelcast.simulator.TestSupport.assertInstanceOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertEquals;

public class MetronomeBuilderTest {

    @Test
    public void withRatePerSecond() {
        // 100 per second is once every 10 ms
        MetronomeBuilder builder = new MetronomeBuilder()
                .withRatePerSecond(100);

        assertEquals(MILLISECONDS.toNanos(10), builder.getIntervalNanos());
    }

    @Test
    public void whenZeroInterval() {
        MetronomeBuilder builder = new MetronomeBuilder()
                .withIntervalMicros(0);

        assertInstanceOf(EmptyMetronome.class, builder.build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNegativeIntervalMicros() {
        new MetronomeBuilder()
                .withIntervalMicros(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNegativeIntervalMillis() {
        new MetronomeBuilder()
                .withIntervalMillis(-1);
    }
}
