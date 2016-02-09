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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.Set;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;
import static org.junit.Assert.assertEquals;

/**
 * A test that verifies the IMap.keySet() or IMap.keySet(true-predicate) behavior.
 */
public class AllKeySetTest {

    private static final ILogger LOGGER = Logger.getLogger(AllKeySetTest.class);

    // properties
    public String basename = AllKeySetTest.class.getSimpleName();
    // the number of map entries
    public int entryCount = 1000000;
    // the size of the key (in chars, since key is string)
    public int keyLength = 10;
    // the size of the value (in chars, since value is a string)
    public int valueLength = 1000;
    // a switch between using IMap.keySet() or IMap.keySet(true-predicate)
    public boolean usePredicate = false;

    private IMap<String, String> map;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) {
        this.targetInstance = testContext.getTargetInstance();
        this.map = targetInstance.getMap(basename);
    }

    @Teardown
    public void teardown() {
        map.destroy();
        LOGGER.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = true)
    public void warmup() {
        Streamer<String, String> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < entryCount; i++) {
            String key = generateString(keyLength);
            String value = generateString(valueLength);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        @Override
        protected void timeStep() throws Exception {
            Set<String> result;
            if (usePredicate) {
                result = map.keySet(TruePredicate.INSTANCE);
            } else {
                result = map.keySet();
            }

            assertEquals(entryCount, result.size());
        }
    }

    public static void main(String[] args) throws Exception {
        AllKeySetTest test = new AllKeySetTest();
        new TestRunner<AllKeySetTest>(test).withDuration(10).run();
    }
}
