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
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.Employee;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;
import java.util.concurrent.Executor;

public class LongEmployeeMapTest extends HazelcastTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public boolean skipPrefill = false;

    private IMap<Long, Employee> map;
    private final Executor callerRuns = Runnable::run;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        if (skipPrefill) {
            return;
        }

        Random random = new Random();
        Streamer<Long, Employee> streamer = StreamerFactory.getInstance(map);
        for (long key = 0; key < keyDomain; key++) {
            Employee employee = new Employee((int)key, "foo", random.nextInt(100));
            streamer.pushEntry(key, employee);
        }
        streamer.await();
    }

    @TimeStep(prob = -1)
    public Employee get(ThreadState state) {
        return map.get(state.randomKey());
    }

    @TimeStep(prob = 0.1)
    public Employee put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public void setAsync(ThreadState state) {
        map.setAsync(state.randomKey(), state.randomValue()).toCompletableFuture();
    }

    public class ThreadState extends BaseThreadState {
        private int i;

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private Employee randomValue() {
            return new Employee(randomInt(keyDomain), "foo", randomInt(200));

        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
