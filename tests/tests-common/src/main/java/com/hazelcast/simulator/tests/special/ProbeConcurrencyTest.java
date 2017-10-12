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
package com.hazelcast.simulator.tests.special;

import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This test is to debug and check probe results from a very controlled test case.
 * <p>
 * By adjusting the threadCount and maxOperations the invocation count of probes are absolutely predictable.
 */
public class ProbeConcurrencyTest extends AbstractTest {

    // properties
    public int threadCount = 0;
    public int maxOperations = 0;

    @Setup
    public void setUp() {
        logger.info("ThreadCount: " + threadCount + " max operations: " + maxOperations);
    }

    @TimeStep
    public void timeStep(AtomicLong counter) {
        long count = counter.incrementAndGet();

        if (count >= maxOperations) {
            throw new StopException();
        }
    }
}
