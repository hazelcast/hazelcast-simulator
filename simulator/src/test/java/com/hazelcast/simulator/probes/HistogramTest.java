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
package com.hazelcast.simulator.probes;

import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HistogramTest {

    private static final int LATENCY_RECORD_COUNT = 5000;
    private static final int MAX_LATENCY = 30000;

    private final Random random = new Random();

    @Test
    public void testHistogramSerialization() throws Exception {
        Histogram original = new Histogram(MAX_LATENCY, 4);
        populateHistogram(original);

        // serialize
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
        outputStream.writeObject(original);
        byte[] bytes = byteArrayOutputStream.toByteArray();

        // de-serialize
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream);
        Histogram read = (Histogram) inputStream.readObject();

        assertEquals(original, read);
        assertTrue(original.equals(read));
        assertEquals(original.hashCode(), read.hashCode());
        assertEquals(original.getNeededByteBufferCapacity(), read.getNeededByteBufferCapacity());
    }

    private void populateHistogram(Histogram original) {
        for (int i = 0; i < LATENCY_RECORD_COUNT; i++) {
            original.recordValue(random.nextInt(MAX_LATENCY));
        }
    }
}
