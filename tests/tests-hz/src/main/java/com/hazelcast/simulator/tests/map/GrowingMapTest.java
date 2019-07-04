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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.map.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GrowingMapTest extends HazelcastTest {

    // properties
    public int threadCount = 10;
    public int growCount = 10000;
    public boolean usePut = true;
    public boolean useRemove = true;
    public int logFrequency = 10000;
    public boolean removeOnStop = true;
    public boolean readValidation = true;

    private IdGenerator idGenerator;
    private IMap<Long, Long> map;

    @Setup
    public void setup() {
        idGenerator = targetInstance.getIdGenerator(name + ":IdGenerator");
        map = targetInstance.getMap(name);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(name);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        private final long[] keys = new long[growCount];
        private final long[] values = new long[growCount];

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int keyIndex = insert();
                if (readValidation) {
                    read(keyIndex);
                }
                delete(keyIndex);
            }
        }

        private int insert() {
            int insertIteration = 0;
            while (!testContext.isStopped() && insertIteration < growCount) {
                long key = idGenerator.newId();
                long value = random.nextLong();

                keys[insertIteration] = key;
                values[insertIteration] = value;

                if (usePut) {
                    map.put(key, value);
                } else {
                    map.set(key, value);
                }

                insertIteration++;
                if (insertIteration % logFrequency == 0) {
                    logger.info(Thread.currentThread().getName() + " At insert iteration: " + insertIteration);
                }
            }

            logger.info(Thread.currentThread().getName() + " Inserted " + insertIteration + " key/value pairs");
            return insertIteration;
        }

        private void read(int keyIndex) {
            int readIteration = 0;
            while (!testContext.isStopped() && readIteration < keyIndex) {
                long key = keys[readIteration];
                long value = values[readIteration];

                long found = map.get(key);
                if (found != value) {
                    throw new TestException("Unexpected value found");
                }

                readIteration++;
                if (readIteration % logFrequency == 0) {
                    logger.info(Thread.currentThread().getName() + " At read iteration: " + readIteration);
                }
            }
        }

        private void delete(int keyIndex) {
            int deleteIteration = 0;
            while ((!testContext.isStopped() || removeOnStop) && deleteIteration < keyIndex) {
                long key = keys[deleteIteration];
                long value = values[deleteIteration];

                if (useRemove) {
                    long found = map.remove(key);
                    if (found != value) {
                        throw new TestException("Expected: %d, but was %d", value, found);
                    }
                } else {
                    map.delete(key);
                }

                deleteIteration++;
                if (deleteIteration % logFrequency == 0) {
                    logger.info(Thread.currentThread().getName() + " At delete iteration: " + deleteIteration);
                }
            }
        }
    }

    @Verify
    public void verify() {
        if (removeOnStop) {
            assertEquals("Map should be empty, but has size: ", 0, map.size());
            assertTrue("Map should be empty, but has size: ", map.isEmpty());
        }
    }

    @Teardown
    public void teardown() {
        map.destroy();
    }

}
